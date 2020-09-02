// @ts-check

const fs = require('fs');
const axios = require('axios').default;
const config = require('config');
const {
  directories,
  referenceIdLength,
  productOrigins,
  fileFormats,
  messageTypes,
} = require('../util/constants');
const { sendQueueMessage } = require('../util/sqsOperations');
const {
  removeS3Files,
  createRawDownloadKey,
  createProductDownloadKey,
  S3Writes,
} = require('../util/wrapS3');
const { computeHash, generateRef } = require('../util/crypto');
const {
  extractZip,
  checkForFileType,
  createDirectories,
  collapseUnzippedDir,
} = require('../util/filesystemUtil');
const { inspectFileExec, parseOutputPath, parseFileExec } = require('../util/processGeoFile');
const {
  acquireConnection,
  doesHashExist,
  lookupCleanGeoName,
  DBWrites,
} = require('../util/wrapQuery');
const { unwindStack } = require('../util/misc');

exports.processInbox = processInbox;

async function processInbox(ctx, data) {
  ctx.process.push('processInbox');

  await acquireConnection(ctx);
  await createDirectories(ctx, [
    directories.logDir,
    directories.outputDir,
    directories.rawDir,
    directories.unzippedDir,
  ]);

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

  ctx.messageId = data.Messages[0].MessageId;
  ctx.type = messageTypes.INBOX;
  const messagePayload = JSON.parse(data.Messages[0].Body);
  ctx.log.info('Processing Message', { messagePayload });

  ctx.isDryRun = messagePayload.dryRun;
  const isDryRun = messagePayload.dryRun;

  const filename = messagePayload.urlKeyVal.split('/').slice(-1);
  const filePath = `${directories.rawDir + ctx.directoryId}/${filename}`;
  const cleanupS3 = [];

  await downloadFile(ctx, messagePayload.urlKeyVal, filePath);

  try {
    const payload = await runMain(ctx, cleanupS3, filePath, isDryRun, messagePayload);

    if (!isDryRun) {
      // payload may be null if state-level dataset.  no derivative products for state.
      if (payload) {
        // Send SQS message to create products
        const productsQueueUrl = config.get('SQS.productQueueUrl');
        await sendQueueMessage(ctx, productsQueueUrl, payload);
      }
    } else {
      ctx.log.info(
        `Because this was a dryRun no database records or S3 files have been written, and no derivative SQS messages have been sent.`,
      );
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
    // rethrow to send email
    throw new Error('Error in main function.  See logs.');
  }
  unwindStack(ctx.process, 'processInbox');
}

async function runMain(ctx, cleanupS3, filePath, isDryRun, messagePayload) {
  ctx.process.push('runMain');

  const sourceNameInput = messagePayload.sourceVal;

  const computedHash = await computeHash(ctx, filePath);

  // call database and find out if hash is already in DB
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

  collapseUnzippedDir(ctx); // this is sync

  // determines if file(s) are of type shapefile or geodatabase
  // only those two types are accomodated right now
  const [fileName, fileType] = await checkForFileType(ctx);

  const inputPath = `${directories.unzippedDir + ctx.directoryId}/${
    fileName + (fileType === 'shapefile' ? '.shp' : '')
  }`;

  const chosenLayerName = await inspectFileExec(ctx, inputPath);

  // determine where on the local disk the output geo products will be written
  const outputPath = parseOutputPath(ctx, fileName, fileType);

  // process all features and convert them to WGS84 ndgeojson
  // while gathering stats on the data.  Writes ndgeojson and stat files to output.
  await parseFileExec(ctx, outputPath, inputPath, chosenLayerName);

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
      messagePayload,
    );
  }

  const productSqsPayload = {
    dryRun: false,
    products: [
      fileFormats.GEOJSON.label,
      fileFormats.GPKG.label,
      fileFormats.SHP.label,
      fileFormats.TILES.label,
    ],
    productRef,
    productOrigin: productOrigins.ORIGINAL,
    fipsDetails,
    geoid,
    geoName,
    downloadRef,
    downloadId,
    productKey,
  };

  unwindStack(ctx.process, 'runMain');

  // dont create derivative products for state-level datasets
  if (fipsDetails.SUMLEV === '040') {
    return null;
  }

  return productSqsPayload;
}

// https://stackoverflow.com/a/61269447/8896489
async function downloadFile(ctx, fileUrl, outputLocationPath) {
  ctx.process.push('downloadFile');

  ctx.log.info('Downloading file: ', { file: fileUrl });

  const writer = fs.createWriteStream(outputLocationPath);

  return axios({
    method: 'get',
    url: fileUrl,
    responseType: 'stream',
  }).then(response => {
    // ensure that the user can call `then()` only when the file has
    // been downloaded entirely.

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
          ctx.log.info('File downloaded to: ', { outputLocationPath });
          unwindStack(ctx.process, 'downloadFile');
          resolve(true);
        }
        // no need to call the reject here, as it will have been called in the
        //'error' stream;
      });
    });
  });
}
