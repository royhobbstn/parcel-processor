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

exports.fetchSourceIdIfExists = async function (pageName) {
  const query = await querySource(pageName);
  if (query.records.length) {
    return [query.records[0].source_id, query.records[0].source_type];
  }
  return [-1, -1];
};

exports.createSource = async function (sourceName, sourceType) {
  const query = await queryWriteSource(sourceName, sourceType);
  if (query.numberOfRecordsUpdated === 1) {
    return query.insertId;
  }
  throw new Error(`unable to create page record for: ${sourceName}`);
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

exports.constructDownloadRecord = async function (pageId, checkId, computedHash) {
  const query = await queryCreateDownloadRecord(pageId, checkId, computedHash);
  console.log(query);
  if (!query || !query.insertId) {
    throw new Error('unexpected result from create download request');
  }
  console.log('new download record created');
  return query.insertId;
};

exports.createProductRecord = async function (geoid, downloadId) {
  const productType = 1; // original product (not filtered from a different product)

  const query = await queryCreateProductRecord(downloadId, productType, geoid);
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

exports.doesHashExist = async function (connection, computedHash) {
  const [rows] = await queryHash(connection, computedHash);
  if (rows.length) {
    console.log('Hash exists in database.  File has already been processed.\n');
    return true;
  }
  console.log('Hash is unique.  Processing new download.');
  return false;
};

exports.constructDownloadRecord = async function (
  connection,
  sourceId,
  checkId,
  computedHash,
  rawKey,
  downloadRef,
  filePath,
) {
  const originalFilename = path.parse(filePath).base;

  const resultSet = await queryCreateDownloadRecord(
    connection,
    sourceId,
    checkId,
    computedHash,
    rawKey,
    downloadRef,
    originalFilename,
  );
  if (!resultSet || !resultSet[0].insertId) {
    throw new Error('unexpected result from create download request');
  }
  console.log('new download record created');
  return resultSet[0].insertId;
};

exports.lookupCleanGeoName = async function (connection, fipsDetails) {
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

  const [rows] = await queryGeographicIdentifier(connection, geoid);

  if (!rows || !rows.length) {
    throw new Error(
      `No geographic match found.  SUMLEV:${SUMLEV} STATEFIPS:${STATEFIPS} COUNTYFIPS:${COUNTYFIPS} PLACEFIPS:${PLACEFIPS}`,
    );
  }

  const rawGeoName = rows[0].geoname;

  console.log(`Found corresponding geographic area: ${rawGeoName}`);

  // Alter geo name to be s3 key friendly (all non alphanumeric become -)
  const geoName = rawGeoName.replace(/[^a-z0-9]+/gi, '-');

  return { geoid, geoName };
};

exports.getSplittableDownloads = async function (connection, geoid) {
  const [rows] = await queryAllOriginalRecentDownloads(connection, geoid);

  if (!rows) {
    throw new Error(`Problem running getSplittableDownlods Query`);
  }

  return { rows };
};

exports.getCountiesByState = async function (connection, geoid) {
  const [rows] = await queryAllCountiesFromState(connection, geoid);

  if (!rows) {
    throw new Error(`Problem running queryAllCountiesFromState Query`);
  }

  return rows;
};
