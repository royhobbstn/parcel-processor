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
} = require('../primitives/queries');
const { sourceTypes, dispositions } = require('../constants');

exports.checkHealth = async function (connection) {
  try {
    const [rows] = await queryHealth(connection);
    const sum = rows[0]['sum'];
    if (sum !== 2) {
      throw new Error('unexpected response from database health check');
    }
    return true;
  } catch (e) {
    return false;
  }
};

exports.recordSourceCheck = async function (connection, sourceId, sourceType) {
  if (sourceType !== sourceTypes.EMAIL && sourceType !== sourceTypes.WEBPAGE) {
    throw new Error('unexpected sourceType');
  }

  const disposition =
    sourceType === sourceTypes.EMAIL ? dispositions.RECEIVED : dispositions.VIEWED;
  const resultSet = await queryWriteSourceCheck(connection, sourceId, disposition);
  return resultSet[0].insertId;
};

exports.fetchSourceIdIfExists = async function (connection, sourceName) {
  const [rows] = await querySource(connection, sourceName);
  if (rows.length) {
    return [rows[0].source_id, rows[0].source_type];
  }
  return [-1, -1];
};

exports.createSource = async function (connection, sourceName, sourceType) {
  const resultSet = await queryWriteSource(connection, sourceName, sourceType);
  if (resultSet[0].affectedRows === 1) {
    return resultSet[0].insertId;
  }
  throw new Error(`unable to create page record for: ${sourceName}`);
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
  // todo use geoid

  const [rows] = await queryAllOriginalRecentDownloads(connection);

  if (!rows || !rows.length) {
    throw new Error(`No matching records.`);
  }

  return { rows };
};
