const path = require('path');
const {
  queryWriteSourceCheck,
  queryHealth,
  querySource,
  queryWriteSource,
  queryHash,
  queryCreateDownloadRecord,
  queryGeographicIdentifier,
  queryCreateProductRecord,
} = require('../primitives/queries');
const { sourceTypes, dispositions } = require('../constants');

exports.checkHealth = async function () {
  try {
    await queryHealth();
    return true;
  } catch (e) {
    return false;
  }
};

exports.recordSourceCheck = async function (sourceId, sourceType) {
  if (sourceType !== sourceTypes.EMAIL && sourceType !== sourceTypes.WEBPAGE) {
    throw new Error('unexpected sourceType');
  }

  const disposition =
    sourceType === sourceTypes.EMAIL ? dispositions.RECEIVED : dispositions.VIEWED;
  const query = await queryWriteSourceCheck(sourceId, disposition);
  return query.insertId;
};

exports.fetchSourceIdIfExists = async function (sourceName) {
  const query = await querySource(sourceName);
  console.log(`queryPage: ${sourceName}`, query);
  if (query.records.length) {
    return query.records[0].source_id;
  }
  return -1;
};

exports.createSource = async function (sourceName, sourceType) {
  const query = await queryWriteSource(sourceName, sourceType);
  console.log(`createSource: ${sourceName}`, query);
  if (query.numberOfRecordsUpdated === 1) {
    return query.insertId;
  }
  throw new Error(`unable to create page record for: ${pageName}`);
};

exports.doesHashExist = async function () {
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
) {
  const originalFilename = path.parse(filePath).base;

  const query = await queryCreateDownloadRecord(
    sourceId,
    checkId,
    computedHash,
    rawKey,
    downloadRef,
    originalFilename,
  );
  console.log(query);
  if (!query || !query.insertId) {
    throw new Error('unexpected result from create download request');
  }
  console.log('new download record created');
  return query.insertId;
};

exports.createProductRecord = async function (downloadId, productRef, geoid, productKey) {
  const query = await queryCreateProductRecord(
    downloadId,
    productRef,
    'ndgeojson',
    'original',
    geoid,
    productKey,
  );
  console.log(query);

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
