// @ts-check

const fs = require('fs');
const axios = require('axios').default;
const config = require('config');
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

async function processInbox(ctx, data) {
  await acquireConnection(ctx);

  // const messagePayload = {
  //   sourceType: 'webpage',
  //   urlKeyVal: 'https://www.google.com',
  //   sumlevVal: '040',
  //   geoidVal: '15',
  //   sourceVal: 'http://www.planning.com',
  //   dryRun: true,
  //   STATEFIPS: '15',
  //   COUNTYFIPS: '',
  //   PLACEFIPS: '',
  // };

  const messagePayload = JSON.parse(data.Messages[0].Body);
  ctx.log.info('Processing Message', { messagePayload });

  const filename = messagePayload.urlKeyVal.split('/').slice(-1);
  const filePath = `${directories.rawDir}/${filename}`;

  await downloadFile(ctx, messagePayload.urlKeyVal, filePath);

  const cleanupS3 = [];

  const isDryRun = messagePayload.dryRun;

  await doBasicCleanup(ctx, [directories.outputDir, directories.unzippedDir], true);

  try {
    const payload = await runMain(ctx, cleanupS3, filePath, isDryRun, messagePayload);

    if (!isDryRun) {
      await doBasicCleanup(
        ctx,
        [directories.rawDir, directories.outputDir, directories.unzippedDir],
        false,
      );

      // Send SQS message to create products
      const productsQueueUrl = config.get('SQS.productQueueUrl');
      await sendQueueMessage(ctx, productsQueueUrl, payload);
    } else {
      ctx.log.info(
        `\nBecause this was a dryRun no database records or S3 files have been written.  \nYour file remains in the raw directory.\n`,
      );
      await doBasicCleanup(ctx, [directories.outputDir, directories.unzippedDir], false);
    }

    ctx.log.info('Completed successfully.');
  } catch (err) {
    ctx.log.error('Error:', { err: err.message, stack: err.stack });

    if (!isDryRun) {
      try {
        await removeS3Files(ctx, cleanupS3);
        ctx.log.info('All created S3 assets have been deleted');
      } catch (err) {
        ctx.log.error('Unable to delete S3 files', { err: err.message, stack: err.stack });
      }
    }

    await doBasicCleanup(ctx, [directories.outputDir, directories.unzippedDir], false);
    ctx.log.info('output and unzipped directories cleaned.  Original raw file left in place.');
  }
}

async function runMain(ctx, cleanupS3, filePath, isDryRun, messagePayload) {
  const sourceNameInput = messagePayload.sourceVal;

  const computedHash = await computeHash(ctx, filePath);

  // call database and find out if hash is already in DB
  await acquireConnection(ctx);
  const hashExists = await doesHashExist(ctx, computedHash);

  if (hashExists) {
    ctx.log.info(`Hash for ${filePath} already exists.`);
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
  const { geoid, geoName } = await lookupCleanGeoName(ctx, fipsDetails);
  ctx.log.info(`geoName:  ${geoName},  geoid: ${geoid}`);

  const downloadRef = generateRef(ctx, referenceIdLength);

  // Contains ZIP extension.  create the key (path) to be used to store the zipfile in S3
  const rawKey = createRawDownloadKey(ctx, fipsDetails, geoid, geoName, downloadRef);

  await extractZip(ctx, filePath);

  // determines if file(s) are of type shapefile or geodatabase
  const [fileName, fileType] = await checkForFileType(ctx);

  // open file && choose layer with OGR/GDAL
  const [dataset, chosenLayer] = inspectFile(ctx, fileName, fileType);

  // determine where on the local disk the output geo products will be written
  const outputPath = parseOutputPath(ctx, fileName, fileType);

  // process all features and convert them to WGS84 ndgeojson
  // while gathering stats on the data.  Writes ndgeojson and stat files to output.
  await parseFile(ctx, dataset, chosenLayer, fileName, outputPath);

  const productRef = generateRef(ctx, referenceIdLength);
  const individualRef = generateRef(ctx, referenceIdLength);

  // contains no file extension.  Used as base key for -stat.json  and .ndgeojson
  const productKey = createProductDownloadKey(
    ctx,
    fipsDetails,
    geoid,
    geoName,
    downloadRef,
    productRef,
    individualRef,
  );

  let downloadId;

  if (!isDryRun) {
    await S3Writes(ctx, cleanupS3, filePath, rawKey, productKey, outputPath);

    downloadId = await DBWrites(
      ctx,
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

// todo move
// https://stackoverflow.com/a/61269447/8896489
async function downloadFile(ctx, fileUrl, outputLocationPath) {
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
