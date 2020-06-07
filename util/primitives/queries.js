const mysql = require('mysql2/promise');

exports.getConnection = async function () {
  // create the connection to database
  connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    database: 'main',
  });

  return connection;
};

exports.queryHealth = function (connection) {
  return connection.query(`SELECT 1 + 1;`);
};

exports.startTransaction = async function (connection) {
  const response = await connection.query(`START TRANSACTION;`);
  console.log(response);
};

exports.commitTransaction = async function (connection) {
  const response = await connection.query(`COMMIT;`);
  console.log(response);
};

exports.rollbackTransaction = async function (connection) {
  const response = await connection.query(`ROLLBACK;`);
  console.log(response);
};

exports.queryHash = function (connection, computedHash) {
  // queries the database for a given hash
  // (if found, than we already have the most recent download)
  return connection.query(`SELECT * FROM downloads where checksum = ?;`, [computedHash]);
};

exports.querySource = function (connection, sourceName) {
  // queries if a webpage / email address exists in the sources table
  return connection.query('SELECT * FROM sources WHERE source_name = ?;', [sourceName]);
};

exports.queryWriteSource = function (connection, sourceName, sourceType) {
  // writes a new source record in the source table
  return connection.query('INSERT INTO sources(source_name, source_type) VALUES (?, ?);', [
    sourceName,
    sourceType,
  ]);
};

exports.queryWriteSourceCheck = function (connection, sourceId, disposition) {
  // write a 'sourceCheck' record to the database
  // it's a record that a source was checked for a more recent download version
  // it is written whether or not a more recent version was found
  // it is meant to give some guidance as to whether or not to check for
  // a more recent version
  return connection.query('INSERT INTO source_checks(source_id, disposition) VALUES (?, ?);', [
    sourceId,
    disposition,
  ]);
};

exports.queryCreateDownloadRecord = function (
  connection,
  sourceId,
  checkId,
  checksum,
  rawKey,
  downloadRef,
  originalFilename,
) {
  // write a download record to unique identify a downloaded file.
  return connection.query(
    'INSERT INTO downloads(source_id, check_id, checksum, raw_key, download_ref, original_filename) VALUES (?, ?, ?, ?, ?, ?);',
    [sourceId, checkId, checksum, rawKey, downloadRef, originalFilename],
  );
};

exports.queryCreateProductRecord = function (
  connection,
  downloadId,
  productRef,
  productType,
  productOrigin,
  geoid,
  productKey,
) {
  // create product record
  return connection.query(
    'INSERT INTO products(download_id, product_ref, product_type, product_origin, geoid, product_key) VALUES (?, ?, ?, ?, ?, ?);',
    [downloadId, productRef, productType, productOrigin, geoid, productKey],
  );
};

exports.queryGeographicIdentifier = function (connection, geoid) {
  // gather geographic information given a geoid
  return connection.query('SELECT * FROM geographic_identifiers WHERE geoid = ?;', [geoid]);
};
