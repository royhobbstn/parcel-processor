// @ts-check

const path = require('path');
const url = require('url');
const {
  queryWriteSourceCheck,
  queryHealth,
  querySource,
  queryWriteSource,
  queryHash,
  queryCreateDownloadRecord,
  queryGeographicIdentifier,
  queryAllOriginalRecentDownloads,
  queryAllCountiesFromState,
  queryAllOriginalRecentDownloadsWithGeoid,
  queryCreateProductRecord,
  querySourceNamesLike,
  startTransaction,
  commitTransaction,
  rollbackTransaction,
} = require('../primitives/queries');
const { getSourceType } = require('../prompts');
const { sourceTypes, dispositions, fileFormats, productOrigins } = require('../constants');
const { log } = require('../logger');

exports.DBWrites = async function (
  sourceNameInput,
  computedHash,
  rawKey,
  downloadRef,
  filePath,
  productRef,
  geoid,
  productKey,
) {
  // refresh connection just in case the upload /parse took a long time
  await acquireConnection();

  let sourceLine;
  const transactionId = await startTransaction();

  try {
    let [sourceId, sourceType] = await fetchSourceIdIfExists(sourceNameInput);

    if (sourceId === -1) {
      log.info(`Source doesn't exist in database.  Creating a new source.`);
      sourceType = getSourceType(sourceNameInput);
      sourceId = await createSource(sourceNameInput, sourceType, transactionId);
      sourceLine = `Created a new source record for: ${sourceNameInput} of type ${sourceType}`;
    } else {
      sourceLine = `Found source record for: ${sourceNameInput}`;
    }

    // todo, accomodate 'received' disposition by prompt
    const checkId = await recordSourceCheck(sourceId, sourceType, transactionId);

    // if not in database write a record in the download table
    const downloadId = await constructDownloadRecord(
      sourceId,
      checkId,
      computedHash,
      rawKey,
      downloadRef,
      filePath,
      transactionId,
    );

    await queryCreateProductRecord(
      downloadId,
      productRef,
      fileFormats.NDGEOJSON.extension,
      productOrigins.ORIGINAL,
      geoid,
      `${productKey}.ndgeojson`,
      transactionId,
    );

    await commitTransaction(transactionId);
    log.info(sourceLine);
    log.info(`Recorded source check as disposition: 'viewed'`);
    log.info(`created record in 'downloads' table.  ref: ${downloadRef}`);
    log.info(`wrote NDgeoJSON product record.  ref: ${productRef}`);
  } catch (e) {
    log.error(e);
    await rollbackTransaction(transactionId);
    log.error('Database transaction has been rolled back.');
    throw new Error('database transaction failed.  re-throwing to roll back S3 saves.');
  }
};

exports.acquireConnection = acquireConnection;

async function acquireConnection() {
  // ping the database to make sure its up / get it ready
  // after that, keep-alives from data-api-client should do the rest
  const seconds = 10;
  let connected = false;
  do {
    log.info('attempting to connect to database');
    connected = await checkHealth();
    if (!connected) {
      log.info(`attempt failed.  trying again in ${seconds} seconds...`);
      await setPause(seconds * 1000);
    }
  } while (!connected);

  log.info('connected');
}

function setPause(timer) {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve();
    }, timer);
  });
}

async function checkHealth() {
  try {
    await queryHealth();
    return true;
  } catch (e) {
    return false;
  }
}

exports.recordSourceCheck = recordSourceCheck;

async function recordSourceCheck(sourceId, sourceType, transactionId) {
  if (sourceType !== sourceTypes.EMAIL && sourceType !== sourceTypes.WEBPAGE) {
    throw new Error('unexpected sourceType');
  }

  const disposition =
    sourceType === sourceTypes.EMAIL ? dispositions.RECEIVED : dispositions.VIEWED;
  const query = await queryWriteSourceCheck(sourceId, disposition, transactionId);

  return query.insertId;
}

exports.fetchSourceIdIfExists = fetchSourceIdIfExists;

async function fetchSourceIdIfExists(pageName) {
  const query = await querySource(pageName);
  if (query.records.length) {
    return [query.records[0].source_id, query.records[0].source_type];
  }
  return [-1, -1];
}

exports.createSource = createSource;

async function createSource(sourceName, sourceType, transactionId) {
  const query = await queryWriteSource(sourceName, sourceType, transactionId);
  if (query.numberOfRecordsUpdated === 1) {
    return query.insertId;
  }
  throw new Error(`unable to create page record for: ${sourceName}`);
}

exports.doesHashExist = async function (computedHash) {
  const query = await queryHash(computedHash);
  if (query.records.length) {
    log.info('Hash exists in database.  File has already been processed.\n');
    return true;
  }
  log.info('Hash is unique.  Processing new download.');
  return false;
};

exports.constructDownloadRecord = constructDownloadRecord;

async function constructDownloadRecord(
  sourceId,
  checkId,
  computedHash,
  rawKey,
  downloadRef,
  filePath,
  transactionId,
) {
  const originalFilename = path.parse(filePath).base;

  const query = await queryCreateDownloadRecord(
    sourceId,
    checkId,
    computedHash,
    rawKey,
    downloadRef,
    originalFilename,
    transactionId,
  );

  if (!query || !query.insertId) {
    throw new Error('unexpected result from create download request');
  }
  log.info('new download record created');
  return query.insertId;
}

exports.lookupCleanGeoName = async function (fipsDetails) {
  const { SUMLEV, STATEFIPS, COUNTYFIPS, PLACEFIPS } = fipsDetails;

  let geoid;

  if (SUMLEV === '040') {
    geoid = STATEFIPS;
  } else if (SUMLEV === '050') {
    geoid = `${STATEFIPS}${COUNTYFIPS}`;
  } else if (SUMLEV === '160') {
    geoid = `${STATEFIPS}${PLACEFIPS}`;
  } else {
    log.error('SUMLEV out of range.  Exiting.');
    throw new Error('SUMLEV out of range');
  }

  const query = await queryGeographicIdentifier(geoid);
  log.info(query);

  if (!query || !query.records || !query.records.length) {
    throw new Error(
      `No geographic match found.  SUMLEV:${SUMLEV} STATEFIPS:${STATEFIPS} COUNTYFIPS:${COUNTYFIPS} PLACEFIPS:${PLACEFIPS}`,
    );
  }

  const rawGeoName = query.records[0].geoname;

  log.info(`Found corresponding geographic area: ${rawGeoName}`);

  // Alter geo name to be s3 key friendly (all non alphanumeric become -)
  const geoName = rawGeoName.replace(/[^a-z0-9]+/gi, '-');

  return { geoid, geoName };
};

exports.getSplittableDownloads = async function (geoid) {
  let query;

  if (geoid) {
    query = await queryAllOriginalRecentDownloadsWithGeoid(geoid);
  } else {
    query = await queryAllOriginalRecentDownloads();
  }

  if (!query) {
    throw new Error(`Problem running getSplittableDownlods Query`);
  }

  return { rows: query.records };
};

exports.getCountiesByState = async function (geoid) {
  const query = await queryAllCountiesFromState(geoid);
  if (!query) {
    throw new Error(`Problem running queryAllCountiesFromState Query`);
  }
  return query.records;
};

exports.querySourceNames = async function (sourceName) {
  // take root of http source, or domain of email sources

  // figure if email or webpage
  let query;

  if (sourceName.includes('@')) {
    const domain = sourceName.split('@');
    query = await querySourceNamesLike(domain[1]);
  } else if (
    sourceName.includes('http://') ||
    sourceName.includes('https://') ||
    sourceName.includes('ftp://') ||
    sourceName.includes('ftps://')
  ) {
    const root = url.parse(sourceName).hostname;
    query = await querySourceNamesLike(root);
  } else {
    console.error('invalid sourceName input in querySourceNames');
    return [];
  }

  if (!query) {
    throw new Error(`Problem running querySourceNames Query`);
  }
  return query.records;
};
