// @ts-check

const fs = require('fs');
const axios = require('axios').default;
const config = require('config');
const { log } = require('../util/logger');
const {
  directories,
  referenceIdLength,
  productOrigins,
  fileFormats,
} = require('../util/constants');
const { doBasicCleanup } = require('../util/cleanup');
const { sendQueueMessage } = require('../util/sqsOperations');
const {
  removeS3Files,
  createRawDownloadKey,
  createProductDownloadKey,
  S3Writes,
} = require('../util/wrapS3');
const { computeHash, generateRef } = require('../util/crypto');
const { extractZip, checkForFileType } = require('../util/filesystemUtil');
const { inspectFile, parseOutputPath, parseFile } = require('../util/processGeoFile');
const {
  acquireConnection,
  doesHashExist,
  lookupCleanGeoName,
  DBWrites,
} = require('../util/wrapQuery');

exports.processInbox = processInbox;

async function processInbox(data) {
  // {
  //   sourceType: 'webpage',
  //   urlKeyVal: 'https://www.google.com',
  //   sumlevVal: '040',
  //   geoidVal: '15',
  //   sourceVal: 'http://www.planning.com',
  //   dryRun: true,
  //   STATEFIPS: '15',
  //   COUNTYFIPS: '',
  //   PLACEFIPS: ''
  // }

  const messagePayload = JSON.parse(data.Messages[0].Body);
  console.log(messagePayload);

  //

  // todo download file

  const filename = messagePayload.urlKeyVal.split('/').slice(-1);
  const filePath = `${directories.rawDir}/${filename}`;

  await downloadFile(messagePayload.urlKeyVal, filePath);

  const cleanupS3 = [];

  const isDryRun = messagePayload.dryRun;

  await doBasicCleanup([directories.outputDir, directories.unzippedDir], true, true);

  try {
    const payload = await runMain(cleanupS3, filePath, isDryRun, messagePayload);

    if (!isDryRun) {
      await doBasicCleanup([directories.rawDir, directories.outputDir, directories.unzippedDir]);

      // Send SQS message to create products
      const productsQueueUrl = config.get('SQS.productQueueUrl');
      await sendQueueMessage(productsQueueUrl, payload);
    } else {
      log.info(
        `\nBecause this was a dryRun no database records or S3 files have been written.  \nYour file remains in the raw directory.\n`,
      );
      await doBasicCleanup([directories.outputDir, directories.unzippedDir]);
    }

    log.info('Completed successfully.');
  } catch (err) {
    log.error(err);

    if (!isDryRun) {
      try {
        await removeS3Files(cleanupS3);
        log.error('All created S3 assets have been deleted');
      } catch (e) {
        log.error(e);
        log.error('Unable to delete S3 files.');
      }
    }

    await doBasicCleanup([directories.outputDir, directories.unzippedDir], true);
    log.info('output and unzipped directories cleaned.  Original raw file left in place.');
  }
}

async function runMain(cleanupS3, filePath, isDryRun, messagePayload) {
  const sourceNameInput = messagePayload.sourceVal;

  const computedHash = await computeHash(filePath);

  // call database and find out if hash is already in DB
  await acquireConnection();
  const hashExists = await doesHashExist(computedHash);

  if (hashExists) {
    log.info(`Hash for ${filePath} already exists.`);
    return;
  }

  // get SUMLEV, STATEFIPS, COUNTYFIPS, PLACEFIPS
  const fipsDetails = {
    SUMLEV: messagePayload.sumlevVal,
    STATEFIPS: messagePayload.STATEFIPS,
    COUNTYFIPS: messagePayload.COUNTYFIPS,
    PLACEFIPS: messagePayload.PLACEFIPS,
  };

  // get geoname corresponding to FIPS
  const { geoid, geoName } = await lookupCleanGeoName(fipsDetails);
  log.info(`geoName:  ${geoName},  geoid: ${geoid}`);

  const downloadRef = generateRef(referenceIdLength);

  // Contains ZIP extension.  create the key (path) to be used to store the zipfile in S3
  const rawKey = createRawDownloadKey(fipsDetails, geoid, geoName, downloadRef);

  await extractZip(filePath);

  // determines if file(s) are of type shapefile or geodatabase
  const [fileName, fileType] = await checkForFileType();

  // open file && choose layer with OGR/GDAL
  const [dataset, chosenLayer] = inspectFile(fileName, fileType);

  // determine where on the local disk the output geo products will be written
  const outputPath = parseOutputPath(fileName, fileType);

  // process all features and convert them to WGS84 ndgeojson
  // while gathering stats on the data.  Writes ndgeojson and stat files to output.
  await parseFile(dataset, chosenLayer, fileName, outputPath);

  const productRef = generateRef(referenceIdLength);
  const individualRef = generateRef(referenceIdLength);

  // contains no file extension.  Used as base key for -stat.json  and .ndgeojson
  const productKey = createProductDownloadKey(
    fipsDetails,
    geoid,
    geoName,
    downloadRef,
    productRef,
    individualRef,
  );

  let downloadId;

  if (!isDryRun) {
    await S3Writes(cleanupS3, filePath, rawKey, productKey, outputPath);

    downloadId = await DBWrites(
      sourceNameInput,
      computedHash,
      rawKey,
      downloadRef,
      filePath,
      productRef,
      geoid,
      productKey,
      individualRef,
    );
  }

  const productSqsPayload = {
    dryRun: false,
    products: [fileFormats.GEOJSON.label, fileFormats.GPKG.label, fileFormats.SHP.label],
    productRef,
    productOrigin: productOrigins.ORIGINAL,
    fipsDetails,
    geoid,
    geoName,
    downloadRef,
    downloadId,
    productKey,
  };

  // dont generate tiles for state-level datasets
  if (fipsDetails.SUMLEV !== '040') {
    productSqsPayload.products.push(fileFormats.TILES.label);
  }

  return productSqsPayload;
}

// https://stackoverflow.com/a/61269447/8896489
async function downloadFile(fileUrl, outputLocationPath) {
  console.log({ fileUrl, outputLocationPath });

  const writer = fs.createWriteStream(outputLocationPath);

  return axios({
    method: 'get',
    url: fileUrl,
    responseType: 'stream',
  }).then(response => {
    //ensure that the user can call `then()` only when the file has
    //been downloaded entirely.

    return new Promise((resolve, reject) => {
      response.data.pipe(writer);
      let error = null;
      writer.on('error', err => {
        error = err;
        writer.close();
        reject(err);
      });
      writer.on('close', () => {
        if (!error) {
          resolve(true);
        }
        //no need to call the reject here, as it will have been called in the
        //'error' stream;
      });
    });
  });
}
