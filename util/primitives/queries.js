const config = require('config');

const slsAuroraClient = require('data-api-client')({
  secretArn: config.get('secretArn'),
  resourceArn: config.get('resourceArn'),
  database: config.get('database'),
  region: config.get('region'),
});

exports.queryHealth = function () {
  return slsAuroraClient.query(`SELECT 1 + 1;`);
};

exports.queryHash = function (computedHash) {
  // queries the database for a given hash
  // (if found, than we already have the most recent download)
  return slsAuroraClient.query(`SELECT * FROM downloads where checksum = :computedHash;`, {
    computedHash,
  });
};

exports.querySource = function (sourceName) {
  // queries if a webpage / email address exists in the sources table
  return slsAuroraClient.query('SELECT * FROM sources WHERE source_name = :sourceName;', {
    sourceName,
  });
};

exports.queryWriteSource = function (sourceName, sourceType) {
  // writes a new source record in the source table
  return slsAuroraClient.query(
    'INSERT INTO sources(source_name, source_type) VALUES (:sourceName, :sourceType);',
    { sourceName, sourceType },
  );
};

exports.queryWriteSourceCheck = function (sourceId, disposition) {
  // write a 'sourceCheck' record to the database
  // it's a record that a source was checked for a more recent download version
  // it is written whether or not a more recent version was found
  // it is meant to give some guidance as to whether or not to check for
  // a more recent version
  return slsAuroraClient.query(
    'INSERT INTO source_checks(source_id, disposition) VALUES (:sourceId, :disposition);',
    { sourceId, disposition },
  );
};

exports.queryCreateDownloadRecord = function (
  sourceId,
  checkId,
  checksum,
  rawKey,
  downloadRef,
  originalFilename,
) {
  // write a download record to unique identify a downloaded file.
  return slsAuroraClient.query(
    'INSERT INTO downloads(source_id, check_id, checksum, raw_key, download_ref, original_filename) VALUES (:sourceId, :checkId, :checksum, :rawKey, :downloadRef, :originalFilename);',
    { sourceId, checkId, checksum, rawKey, downloadRef, originalFilename },
  );
};

exports.queryCreateProductRecord = function (
  downloadId,
  productRef,
  productType,
  productOrigin,
  geoid,
  productKey,
) {
  // create product record
  return slsAuroraClient.query(
    'INSERT INTO products(download_id, product_ref, product_type, product_origin, geoid, product_key) VALUES (:downloadId, :productRef, :productType, :productOrigin, :geoid, :productKey);',
    { downloadId, productRef, productType, productOrigin, geoid, productKey },
  );
};

exports.queryGeographicIdentifier = function (geoid) {
  // gather geographic information given a geoid
  return slsAuroraClient.query('SELECT * FROM geographic_identifiers WHERE geoid = :geoid;', {
    geoid,
  });
};
