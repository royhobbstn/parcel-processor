// @ts-check
require('wise-inspection')(Promise); // augments promises with inspection
const config = require('config');
const path = require('path');
const fs = require('fs');
const ndjson = require('ndjson');
const { generateRef } = require('../util/crypto');
const { unwindStack, getTimestamp, isNewEngland } = require('../util/misc');
const { countStats } = require('../util/processGeoFile');
const {
  directories,
  fileFormats,
  productOrigins,
  referenceIdLength,
  s3deleteType,
  messageTypes,
} = require('../util/constants');
const {
  queryCreateProductRecord,
  checkForProduct,
  createMessageRecord,
} = require('../util/queries');
const { makeS3Key, acquireConnection, lookupCleanGeoName } = require('../util/wrapQuery');
const { createProductDownloadKey, removeS3Files } = require('../util/wrapS3');
const { putFileToS3, streamS3toFileSystem } = require('../util/s3Operations');
const { sendQueueMessage } = require('../util/sqsOperations');
const { createDirectories } = require('../util/filesystemUtil');

exports.processSort = processSort;

async function processSort(ctx, data) {
  ctx.process.push({ name: 'processSort', timestamp: getTimestamp() });

  await acquireConnection(ctx);

  await createDirectories(ctx, [
    directories.logDir,
    directories.subGeographiesDir,
    directories.productTempDir,
  ]);

  // const messagePayload = {
  //   dryRun: true,
  //   selectedFieldKey: 'county',
  //   selectedDownload: {
  //     geoid: '15',
  //     geoname: 'Hawaii',
  //     source_name: 'http://planning.hawaii.gov/gis/download-gis-data/',
  //     source_type: 'webpage',
  //     download_id: 3,
  //     download_ref: '5fe3a581',
  //     product_id: 11,
  //     product_ref: '65aa145b',
  //     last_checked: '2020-06-21 16:11:24',
  //     product_key: '15-Hawaii/000-Hawaii/5fe3a581-65aa145b-15-Hawaii.ndgeojson',
  //     original_filename: 'tmk_state.shp.zip',
  //   },
  //   modalStatsObj: {
  //     missingAttributes: [],
  //     missingGeoids: ['15005'],
  //     countOfPossible: 5,
  //     countOfUniqueGeoids: 4,
  //     attributesUsingSameGeoid: [],
  //     mapping: {
  //       Hawaii: '15001',
  //       Honolulu: '15003',
  //       Kauai: '15007',
  //       Maui: '15009',
  //     },
  //   },
  //   geographies: [
  //     { geoid: '15001', geoname: 'Hawaii County' },
  //     { geoid: '15003', geoname: 'Honolulu County' },
  //     { geoid: '15005', geoname: 'Kalawao County' },
  //     { geoid: '15007', geoname: 'Kauai County' },
  //     { geoid: '15009', geoname: 'Maui County' },
  //   ],
  // };

  ctx.messageId = data.Messages[0].MessageId;
  ctx.type = messageTypes.SORT;
  const messagePayload = JSON.parse(data.Messages[0].Body);
  ctx.log.info('Processing Message', { messagePayload });

  ctx.isDryRun = messagePayload.dryRun;
  const isDryRun = messagePayload.dryRun;
  const selectedFieldKey = messagePayload.selectedFieldKey;
  const geoidTranslator = messagePayload.modalStatsObj.mapping;
  const downloadId = messagePayload.selectedDownload.download_id;
  const downloadRef = messagePayload.selectedDownload.download_ref;
  const geonameLookup = keifyGeographies(ctx, messagePayload.geographies);
  const files = Array.from(new Set(Object.values(geoidTranslator)));

  const productKey = messagePayload.selectedDownload.product_key;
  const fileNameBase = productKey.split('/').slice(-1)[0];
  const destPlain = `${directories.productTempDir + ctx.directoryId}/${fileNameBase}.gz`;
  const destUnzipped = `${directories.productTempDir + ctx.directoryId}/${fileNameBase}`;

  await sortData(ctx);

  // iterate through files and process each (stat files, s3 and db records)
  for (let file of files) {
    await processFile(ctx, file);
  }

  await createMessageRecord(ctx, ctx.messageId, JSON.stringify(messagePayload), ctx.type);
  ctx.log.info('Message reference record was created.');

  ctx.log.info('done with processSort');
  unwindStack(ctx, 'processSort');

  // ---- functions only below

  async function sortData(ctx) {
    ctx.process.push({ name: 'sortData', timestamp: getTimestamp() });

    try {
      ctx.log.info('Beginning file download from S3.');
      await streamS3toFileSystem(
        ctx,
        config.get('Buckets.productsBucket'),
        `${productKey}`,
        destPlain,
        destUnzipped,
      );
    } catch (err) {
      ctx.log.info(`Error streaming the file from S3.`, {
        data: err.message,
        stack: err.stack,
      });
      throw err;
    }

    let fileWrites = [];
    let counter = 0;

    await new Promise((resolve, reject) => {
      fs.createReadStream(`${destUnzipped}`)
        .pipe(ndjson.parse({ strict: false }))
        .on('data', async function (obj) {
          counter++;

          if (counter % 10000 === 0) {
            // filter out completed writes.  Otherwise, on large files:
            // RangeError: Too many elements passed to Promise.all
            fileWrites = fileWrites.filter(pr => {
              const promiseStatus = pr.inspect();
              return promiseStatus === 'pending';
            });
            ctx.log.info(counter);
          }

          const splitValue = obj.properties[selectedFieldKey];
          const geoidFileTranslate = geoidTranslator[splitValue];
          const fullPathFilename = `${
            directories.subGeographiesDir + ctx.directoryId
          }/${geoidFileTranslate}`;

          // stuff json line into a file depending on the county
          fileWrites.push(appendFileAsync(ctx, fullPathFilename, obj));
        })
        .on('error', err => {
          ctx.log.warn('Error', { err: err.message, stack: err.stack });
          return reject(err);
        })
        .on('end', async () => {
          ctx.log.info(counter + ' records sorted');
          return resolve();
        });
    });

    ctx.log.info('done reading main ndgeojson file.  waiting for writes to complete.');
    await Promise.all(fileWrites);
    ctx.log.info(`processed ${counter} lines`);
    unwindStack(ctx, 'sortData');
  }

  async function processFile(ctx, file) {
    ctx.process.push({ name: 'processFile', timestamp: getTimestamp() });

    ctx.log.info(`processing: ${geonameLookup[file]}`);

    // before processing, do a check to make sure there is not already a product with same geoid / downloadRef, productRef combination.
    // SQS can sometimes duplicate messages, and this would guard against it.
    const doesProductExist = await checkForProduct(
      ctx,
      file,
      downloadId,
      fileFormats.NDGEOJSON.extension,
    );

    if (doesProductExist) {
      ctx.log.info(
        `Product ${fileFormats.NDGEOJSON.extension} has already been created for geoid: ${file}.  Skipping.`,
      );
      unwindStack(ctx, 'processFile');
      return;
    }

    // ndgeojson file is a plain geoid with no file extension
    const basePath = `${directories.subGeographiesDir + ctx.directoryId}/${file}`;
    const statPath = `${directories.subGeographiesDir + ctx.directoryId}/${file}.json`;

    const statExport = await countStats(ctx, basePath);
    fs.writeFileSync(statPath, JSON.stringify(statExport), 'utf8');

    // create product ref
    const productRef = generateRef(ctx, referenceIdLength);
    const individualRef = generateRef(ctx, referenceIdLength);

    const state = file.slice(0, 2);
    const sumlev = isNewEngland(state) ? '060' : '050';

    const fipsDetails = createFipsDetailsForArea(ctx, file, sumlev);

    const { geoid, geoName } = await lookupCleanGeoName(ctx, fipsDetails);

    const productKey = createProductDownloadKey(
      ctx,
      fipsDetails,
      geoid,
      geonameLookup[file],
      downloadRef,
      productRef,
      individualRef,
    );

    if (!isDryRun) {
      const cleanupS3 = [];

      try {
        // upload stat file
        await putFileToS3(
          ctx,
          config.get('Buckets.productsBucket'),
          `${productKey}-stat.json`,
          statPath,
          'application/json',
          true,
          false,
        );

        cleanupS3.push({
          bucket: config.get('Buckets.productsBucket'),
          key: `${productKey}-stat.json`,
          type: s3deleteType.FILE,
        });

        // upload ndgeojson file
        await putFileToS3(
          ctx,
          config.get('Buckets.productsBucket'),
          `${productKey}.ndgeojson`,
          basePath,
          'application/geo+json-seq',
          true,
          `attachment; filename="${path.basename(productKey)}.ndgeojson"`,
        );

        cleanupS3.push({
          bucket: config.get('Buckets.productsBucket'),
          key: `${productKey}.ndgeojson`,
          type: s3deleteType.FILE,
        });

        // writes just one record.  no transaction needed.  it works or it doesnt
        await queryCreateProductRecord(
          ctx,
          downloadId,
          productRef,
          individualRef,
          fileFormats.NDGEOJSON.extension,
          productOrigins.DERIVED,
          file,
          `${productKey}.ndgeojson`,
          ctx.messageId,
        );
      } catch (err) {
        await removeS3Files(ctx, cleanupS3);
        // throwing here will cause processSort to end.
        // accomodates resuming if identical message (in terms of data) is sent
        throw err;
      }

      // Send SQS message to create products

      const productsQueueUrl = config.get('SQS.productQueueUrl');
      const payload = {
        dryRun: false,
        products: [
          fileFormats.GEOJSON.label,
          fileFormats.GPKG.label,
          fileFormats.SHP.label,
          fileFormats.TILES.label,
        ],
        productRef,
        productOrigin: productOrigins.DERIVED,
        fipsDetails,
        geoid,
        geoName,
        downloadRef,
        downloadId,
        productKey,
      };
      await sendQueueMessage(ctx, productsQueueUrl, payload);
    }

    unwindStack(ctx, 'processFile');
  }
}

function appendFileAsync(ctx, fullPathFilename, json) {
  return new Promise((resolve, reject) => {
    fs.appendFile(fullPathFilename, JSON.stringify(json) + '\n', err => {
      if (err) {
        return reject(err);
      }
      resolve();
    });
  });
}

function keifyGeographies(ctx, geographies) {
  const obj = {};

  geographies.forEach(geography => {
    obj[geography.geoid] = makeS3Key(ctx, geography.geoname);
  });

  return obj;
}

function createFipsDetailsForArea(ctx, geoid, sumlev) {
  if (sumlev === '050') {
    return {
      SUMLEV: '050',
      STATEFIPS: geoid.slice(0, 2),
      COUNTYFIPS: geoid.slice(2),
      PLACEFIPS: '',
      COUNTYSUBFIPS: '',
    };
  } else if (sumlev === '160') {
    return {
      SUMLEV: '160',
      STATEFIPS: geoid.slice(0, 2),
      COUNTYFIPS: '',
      PLACEFIPS: geoid.slice(2),
      COUNTYSUBFIPS: '',
    };
  } else if (sumlev === '060') {
    return {
      SUMLEV: '060',
      STATEFIPS: geoid.slice(0, 2),
      COUNTYFIPS: '',
      PLACEFIPS: '',
      COUNTYSUBFIPS: geoid.slice(2),
    };
  } else {
    throw new Error('Unexpected or missing summary level');
  }
}
