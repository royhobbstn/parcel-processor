const config = require('config');
const axios = require('axios');
const httpAdapter = require('axios/lib/adapters/http');
const fs = require('fs');
const ndjson = require('ndjson');
const { generateRef } = require('../util/crypto');
const { StatContext } = require('../util/StatContext');
const {
  directories,
  fileFormats,
  productOrigins,
  referenceIdLength,
} = require('../util/constants');
const { queryCreateProductRecord } = require('../util/primitives/queries');
const { makeS3Key, acquireConnection } = require('../util/wrappers/wrapQuery');
const { createProductDownloadKey } = require('../util/wrappers/wrapS3');
const { putFileToS3, putTextToS3 } = require('../util/primitives/s3Operations');
const { doBasicCleanup } = require('../util/cleanup');
const { log } = require('winston');

let counter = 0;

exports.processSort = processSort;

async function processSort(data) {
  await acquireConnection();

  // to avoid uploading anything from a previous run
  await doBasicCleanup([directories.subGeographiesDir], false, true);

  const messagePayload = {
    dryRun: true,
    selectedFieldKey: 'county',
    selectedDownload: {
      geoid: '15',
      geoname: 'Hawaii',
      source_name: 'http://planning.hawaii.gov/gis/download-gis-data/',
      source_type: 'webpage',
      download_id: 3,
      download_ref: '5fe3a581',
      product_id: 11,
      product_ref: '65aa145b',
      last_checked: '2020-06-21 16:11:24',
      product_key: '15-Hawaii/000-Hawaii/5fe3a581-65aa145b-15-Hawaii.ndgeojson',
      original_filename: 'tmk_state.shp.zip',
    },
    modalStatsObj: {
      missingAttributes: [],
      missingGeoids: ['15005'],
      countOfPossible: 5,
      countOfUniqueGeoids: 4,
      attributesUsingSameGeoid: [],
      mapping: {
        Hawaii: '15001',
        Honolulu: '15003',
        Kauai: '15007',
        Maui: '15009',
      },
    },
    geographies: [
      { geoid: '15001', geoname: 'Hawaii County' },
      { geoid: '15003', geoname: 'Honolulu County' },
      { geoid: '15005', geoname: 'Kalawao County' },
      { geoid: '15007', geoname: 'Kauai County' },
      { geoid: '15009', geoname: 'Maui County' },
    ],
  };

  // const messagePayload = JSON.parse(data.Messages[0].Body);
  console.log(messagePayload);

  const isDryRun = messagePayload.dryRun;
  const bucket = config.get('Buckets.productsBucket');
  const selectedFieldKey = messagePayload.selectedFieldKey;
  const remoteFile = `https://${bucket}.s3.us-east-2.amazonaws.com/${messagePayload.selectedDownload.product_key}`;
  const geoidTranslator = messagePayload.modalStatsObj.mapping;
  const downloadId = messagePayload.selectedDownload.download_id;
  const downloadRef = messagePayload.selectedDownload.download_ref;
  const geonameLookup = keifyGeographies(messagePayload.geographies);
  const files = Array.from(new Set(Object.values(geoidTranslator)));
  const fileWrites = [];

  await sortData();

  // iterate through files and process each (stat files, s3 and db records)
  for (let file of files) {
    await processFile(file);
  }
  await doBasicCleanup([directories.subGeographiesDir], false, true);

  log.info('done with processSort');

  // ---- functions only below

  function sortData() {
    return new Promise((resolve, reject) => {
      const dataToProcess = [];
      let lastChunkPiece = '';

      // request the file
      axios.get(remoteFile, { responseType: 'stream', adapter: httpAdapter }).then(response => {
        const stream = response.data;
        stream.on('data', chunk => {
          const rawData = new Buffer.from(chunk).toString();
          const splitData = rawData.split('\n');

          if (splitData.length === 0) {
          } else if (splitData.length === 1) {
            lastChunkPiece = lastChunkPiece + splitData[0];
          } else {
            dataToProcess.push(JSON.parse(lastChunkPiece + splitData[0]));

            for (let s = 1; s < splitData.length - 1; s++) {
              dataToProcess.push(JSON.parse(splitData[s]));
            }
            lastChunkPiece = splitData[splitData.length - 1];
          }

          processData(dataToProcess);
        });
        stream.on('error', err => {
          log.error(err);
          return reject(err);
        });
        stream.on('end', async () => {
          // might only happen if there is no newline after last record
          if (lastChunkPiece) {
            dataToProcess.push(JSON.parse(lastChunkPiece));
            processData(dataToProcess);
          }

          console.log('done reading remote file.');
          await Promise.all(fileWrites);
          console.log(`processed ${counter} lines`);

          return resolve();
        });
      });
    });
  }

  function processData(dataArr) {
    while (dataArr.length) {
      processLine(dataArr.pop());
    }
  }

  async function processLine(obj) {
    counter++;

    if (counter % 1000 === 0) {
      console.log(counter);
    }

    const splitValue = obj.properties[selectedFieldKey];
    const geoidFileTranslate = geoidTranslator[splitValue];
    const fullPathFilename = `${directories.subGeographiesDir}/${geoidFileTranslate}`;

    // stuff json line into a file depending on the county
    fileWrites.push(appendFileAsync(fullPathFilename, obj));
  }

  async function processFile(file) {
    console.log(`processing: ${geonameLookup[file]}`);
    // file is a plain geoid with no file extension
    const path = `${directories.subGeographiesDir}/${file}`;

    const statExport = await countStats(path);

    // create product ref
    const productRef = generateRef(referenceIdLength);
    const individualRef = generateRef(referenceIdLength);

    const productKey = createProductDownloadKey(
      createFipsDetailsForCounty(file),
      file,
      geonameLookup[file],
      downloadRef,
      productRef,
      individualRef,
    );

    if (!isDryRun) {
      // async operations done in parallel
      const operations = [];

      // upload stat file
      operations.push(
        putTextToS3(
          config.get('Buckets.productsBucket'),
          `${productKey}-stat.json`,
          JSON.stringify(statExport),
          'application/json',
          true,
        ),
      );

      // upload ndgeojson file
      operations.push(
        putFileToS3(
          config.get('Buckets.productsBucket'),
          `${productKey}.ndgeojson`,
          path,
          'application/geo+json-seq',
          true,
        ),
      );

      // write product record
      operations.push(
        queryCreateProductRecord(
          downloadId,
          productRef,
          fileFormats.NDGEOJSON.extension,
          productOrigins.DERIVED,
          file,
          `${productKey}.ndgeojson`,
        ),
      );

      await Promise.all(operations);

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
      await sendQueueMessage(productsQueueUrl, payload);
    }
  }
}

function appendFileAsync(fullPathFilename, json) {
  return new Promise((resolve, reject) => {
    fs.appendFile(fullPathFilename, JSON.stringify(json) + '\n', err => {
      if (err) {
        return reject(err);
      }
      resolve();
    });
  });
}

function keifyGeographies(geographies) {
  const obj = {};

  geographies.forEach(geography => {
    obj[geography.geoid] = makeS3Key(geography.geoname);
  });

  return obj;
}

function createFipsDetailsForCounty(geoid) {
  return {
    SUMLEV: '050',
    STATEFIPS: geoid.slice(0, 2),
    COUNTYFIPS: geoid.slice(2),
    PLACEFIPS: '',
  };
}

function countStats(path) {
  return new Promise((resolve, reject) => {
    // read each file as ndjson and create stat object
    let transformed = 0;
    const statCounter = new StatContext();

    fs.createReadStream(path)
      .pipe(ndjson.parse())
      .on('data', function (obj) {
        statCounter.countStats(obj);

        transformed++;
        if (transformed % 10000 === 0) {
          console.log(transformed + ' records processed');
        }
      })
      .on('error', err => {
        console.error(err);
        return reject(err);
      })
      .on('end', async () => {
        console.log(transformed + ' records processed');

        return resolve(statCounter.export());
      });
  });
}
