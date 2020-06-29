// @ts-check
const config = require('config');
const { generateRef } = require('./crypto');
const { referenceIdLength } = require('./constants');

const slsAuroraClient = require('data-api-client')({
  secretArn: config.get('RDS.secretArn'),
  resourceArn: config.get('RDS.resourceArn'),
  database: config.get('RDS.database'),
  region: config.get('RDS.region'),
});

exports.queryHealth = function () {
  return slsAuroraClient.query(`SELECT 1 + 1 AS sum;`);
};

exports.queryHash = function (computedHash) {
  // queries the database for a given hash
  // (if found, than we already have the most recent download)
  return slsAuroraClient.query(`SELECT * FROM downloads where checksum = :computedHash;`, {
    computedHash,
  });
};

exports.querySource = function (sourceName) {
  // queries if a source exists in the source table
  return slsAuroraClient.query('SELECT * FROM sources WHERE source_name = :sourceName;', {
    sourceName,
  });
};

exports.queryWriteSource = function (sourceName, sourceType, transactionId) {
  // writes a new source record in the source table
  return slsAuroraClient.query({
    sql: 'INSERT INTO sources(source_name, source_type) VALUES (:sourceName, :sourceType);',
    parameters: {
      sourceName,
      sourceType,
    },
    transactionId,
  });
};

exports.queryWriteSourceCheck = function (sourceId, disposition, transactionId) {
  // write a 'sourceCheck' record to the database
  // it's a record that a source was checked for a more recent download version
  // it is written whether or not a more recent version was found
  // it is meant to give some guidance as to whether or not to check for
  // a more recent version
  return slsAuroraClient.query({
    sql: 'INSERT INTO source_checks(source_id, disposition) VALUES (:sourceId, :disposition);',
    parameters: { sourceId, disposition },
    transactionId,
  });
};

exports.queryCreateDownloadRecord = function (
  sourceId,
  checkId,
  checksum,
  rawKey,
  downloadRef,
  originalFilename,
  transactionId,
) {
  // write a download record to unique identify a downloaded file.
  return slsAuroraClient.query({
    sql:
      'INSERT INTO downloads(source_id, check_id, checksum, raw_key, download_ref, original_filename) VALUES (:sourceId, :checkId, :checksum, :rawKey, :downloadRef, :originalFilename);',
    parameters: { sourceId, checkId, checksum, rawKey, downloadRef, originalFilename },
    transactionId,
  });
};

exports.queryCreateProductRecord = function (
  downloadId,
  productRef,
  individualRef,
  productType,
  productOrigin,
  geoid,
  productKey,
  transactionId,
) {
  return slsAuroraClient.query({
    sql:
      'INSERT INTO products(download_id, product_ref, individual_ref, product_type, product_origin, geoid, product_key) VALUES (:downloadId, :productRef, :individualRef, :productType, :productOrigin, :geoid, :productKey);',
    parameters: {
      downloadId,
      productRef,
      individualRef,
      productType,
      productOrigin,
      geoid,
      productKey,
    },
    transactionId,
  });
};

exports.queryGeographicIdentifier = function (geoid) {
  return slsAuroraClient.query('SELECT * FROM geographic_identifiers WHERE geoid = :geoid;', {
    geoid,
  });
};

exports.queryAllCountiesFromState = function (geoid) {
  return slsAuroraClient.query({
    sql: `SELECT geoid, geoname FROM geographic_identifiers WHERE LEFT(geoid, 2) = :geoid and sumlev = "050" order by geoname asc;`,
    parameters: {
      geoid,
    },
  });
};

exports.queryAllOriginalRecentDownloads = function () {
  return slsAuroraClient.query(
    'select geographic_identifiers.geoid, geoname, source_name, source_type, downloads.download_id, download_ref, product_id, product_ref, last_checked, product_key, original_filename  from downloads left join products on products.download_id = downloads.download_id join source_checks on source_checks.check_id = downloads.check_id join sources on sources.source_id = downloads.source_id join geographic_identifiers on geographic_identifiers.geoid = products.geoid where products.product_type = "ndgeojson" and products.product_origin="original"',
  );
};

exports.queryAllOriginalRecentDownloadsWithGeoid = function (geoid) {
  return slsAuroraClient.query({
    sql:
      'select geographic_identifiers.geoid, geoname, source_name, source_type, downloads.download_id, download_ref, product_id, product_ref, last_checked, product_key, original_filename  from downloads left join products on products.download_id = downloads.download_id join source_checks on source_checks.check_id = downloads.check_id join sources on sources.source_id = downloads.source_id join geographic_identifiers on geographic_identifiers.geoid = products.geoid where products.product_type = "ndgeojson" and products.product_origin="original" and geographic_identifiers.geoid = :geoid',
    parameters: { geoid },
  });
};

exports.querySourceNamesLike = function (sourceName) {
  return slsAuroraClient.query({
    sql: 'SELECT * from sources WHERE source_name LIKE :sourceName',
    parameters: { sourceName: `%${sourceName}%` },
  });
};

exports.startTransaction = async function () {
  const query = await slsAuroraClient.beginTransaction();
  return query.transactionId;
};

exports.commitTransaction = function (transactionId) {
  return slsAuroraClient.commitTransaction({ transactionId });
};

exports.rollbackTransaction = async function (transactionId) {
  return slsAuroraClient.rollbackTransaction({ transactionId });
};
