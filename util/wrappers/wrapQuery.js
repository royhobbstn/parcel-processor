// @ts-check

const path = require('path');
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
} = require('../primitives/queries');
const { sourceTypes, dispositions } = require('../constants');

exports.acquireConnection = async function () {
  // ping the database to make sure its up / get it ready
  // after that, keep-alives from data-api-client should do the rest
  const seconds = 10;
  let connected = false;
  do {
    console.log('attempting to connect to database');
    connected = await checkHealth();
    if (!connected) {
      console.log(`attempt failed.  trying again in ${seconds} seconds...`);
      await setPause(seconds * 1000);
    }
  } while (!connected);

  console.log('connected');
};

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

exports.recordSourceCheck = async function (sourceId, sourceType, transactionId) {
  if (sourceType !== sourceTypes.EMAIL && sourceType !== sourceTypes.WEBPAGE) {
    throw new Error('unexpected sourceType');
  }

  const disposition =
    sourceType === sourceTypes.EMAIL ? dispositions.RECEIVED : dispositions.VIEWED;
  const query = await queryWriteSourceCheck(sourceId, disposition, transactionId);

  return query.insertId;
};

exports.fetchSourceIdIfExists = async function (pageName) {
  const query = await querySource(pageName);
  if (query.records.length) {
    return [query.records[0].source_id, query.records[0].source_type];
  }
  return [-1, -1];
};

exports.createSource = async function (sourceName, sourceType, transactionId) {
  const query = await queryWriteSource(sourceName, sourceType, transactionId);
  if (query.numberOfRecordsUpdated === 1) {
    return query.insertId;
  }
  throw new Error(`unable to create page record for: ${sourceName}`);
};

exports.doesHashExist = async function (computedHash) {
  const query = await queryHash(computedHash);
  if (query.records.length) {
    console.log('Hash exists in database.  File has already been processed.\n');
    return true;
  }
  console.log('Hash is unique.  Processing new download.');
  return false;
};

exports.constructDownloadRecord = async function (
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
  console.log('new download record created');
  return query.insertId;
};

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
    console.error('SUMLEV out of range.  Exiting.');
    process.exit();
  }

  const query = await queryGeographicIdentifier(geoid);
  console.log(query);

  if (!query || !query.records || !query.records.length) {
    throw new Error(
      `No geographic match found.  SUMLEV:${SUMLEV} STATEFIPS:${STATEFIPS} COUNTYFIPS:${COUNTYFIPS} PLACEFIPS:${PLACEFIPS}`,
    );
  }

  const rawGeoName = query.records[0].geoname;

  console.log(`Found corresponding geographic area: ${rawGeoName}`);

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

  return { rows: query.records };
};
