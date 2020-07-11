// @ts-check
const config = require('config');

const slsAuroraClient = require('data-api-client')({
  secretArn: config.get('RDS.secretArn'),
  resourceArn: config.get('RDS.resourceArn'),
  database: config.get('RDS.database'),
  region: config.get('RDS.region'),
});

exports.queryHealth = function (ctx) {
  return slsAuroraClient.query(`SELECT 1 + 1 AS sum;`);
};

exports.queryHash = function (ctx, computedHash) {
  // queries the database for a given hash
  // (if found, than we already have the most recent download)
  return slsAuroraClient.query(`SELECT * FROM downloads where checksum = :computedHash;`, {
    computedHash,
  });
};

exports.querySource = function (ctx, sourceName) {
  // queries if a source exists in the source table
  return slsAuroraClient.query('SELECT * FROM sources WHERE source_name = :sourceName;', {
    sourceName,
  });
};

exports.queryWriteSource = function (ctx, sourceName, sourceType, transactionId) {
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

exports.queryWriteSourceCheck = function (ctx, sourceId, disposition, transactionId) {
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
  ctx,
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

exports.queryCreateProductRecord = async function (
  ctx,
  downloadId,
  productRef,
  individualRef,
  productType,
  productOrigin,
  geoid,
  productKey,
  transactionId,
) {
  const query = await slsAuroraClient.query({
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

  ctx.log.info('product record created', { record: query });
  const product_id = query.insertId;
  ctx.log.info('ProductId of created record: ' + product_id);

  return product_id;
};

exports.createLogfileRecord = function (
  ctx,
  productId,
  messageId,
  messagePayload,
  messageType,
  transactionId,
) {
  return slsAuroraClient.query({
    sql:
      'INSERT INTO logfiles(product_id, message_id, message_body, message_type) VALUES (:productId, :messageId, :messagePayload, :messageType);',
    parameters: {
      productId,
      messageId,
      messagePayload,
      messageType,
    },
    transactionId,
  });
};

exports.queryGeographicIdentifier = function (ctx, geoid) {
  return slsAuroraClient.query('SELECT * FROM geographic_identifiers WHERE geoid = :geoid;', {
    geoid,
  });
};

exports.queryAllCountiesFromState = function (ctx, geoid) {
  return slsAuroraClient.query({
    sql: `SELECT geoid, geoname FROM geographic_identifiers WHERE LEFT(geoid, 2) = :geoid and sumlev = "050" order by geoname asc;`,
    parameters: {
      geoid,
    },
  });
};

exports.queryAllOriginalRecentDownloads = function (ctx) {
  return slsAuroraClient.query(
    'select geographic_identifiers.geoid, geoname, source_name, source_type, downloads.download_id, download_ref, product_id, product_ref, last_checked, product_key, original_filename  from downloads left join products on products.download_id = downloads.download_id join source_checks on source_checks.check_id = downloads.check_id join sources on sources.source_id = downloads.source_id join geographic_identifiers on geographic_identifiers.geoid = products.geoid where products.product_type = "ndgeojson" and products.product_origin="original"',
  );
};

exports.queryAllOriginalRecentDownloadsWithGeoid = function (ctx, geoid) {
  return slsAuroraClient.query({
    sql:
      'select geographic_identifiers.geoid, geoname, source_name, source_type, downloads.download_id, download_ref, product_id, product_ref, last_checked, product_key, original_filename  from downloads left join products on products.download_id = downloads.download_id join source_checks on source_checks.check_id = downloads.check_id join sources on sources.source_id = downloads.source_id join geographic_identifiers on geographic_identifiers.geoid = products.geoid where products.product_type = "ndgeojson" and products.product_origin="original" and geographic_identifiers.geoid = :geoid',
    parameters: { geoid },
  });
};

exports.querySourceNamesLike = function (ctx, sourceName) {
  return slsAuroraClient.query({
    sql: 'SELECT * from sources WHERE source_name LIKE :sourceName',
    parameters: { sourceName: `%${sourceName}%` },
  });
};

exports.startTransaction = async function (ctx) {
  const query = await slsAuroraClient.beginTransaction();
  return query.transactionId;
};

exports.commitTransaction = function (ctx, transactionId) {
  return slsAuroraClient.commitTransaction({ transactionId });
};

exports.rollbackTransaction = function (ctx, transactionId) {
  return slsAuroraClient.rollbackTransaction({ transactionId });
};

exports.checkForProduct = async function (ctx, geoid, downloadId, format, returnBool) {
  const query = await slsAuroraClient.query({
    sql:
      'select * from products where products.geoid = :geoid and products.download_id = :downloadId and products.product_type = :format',
    parameters: { geoid, downloadId, format },
  });
  return query.records.length > 0;
};

exports.checkForProducts = async function (ctx, geoid, downloadId) {
  const query = await slsAuroraClient.query({
    sql:
      'select * from products where products.geoid = :geoid and products.download_id = :downloadId',
    parameters: { geoid, downloadId },
  });
  return query.records;
};

exports.searchLogsByType = function (ctx, type) {
  let clause = '';
  if (type !== 'all') {
    clause = 'WHERE message_type = :type';
  }
  return slsAuroraClient.query({
    sql:
      'SELECT product_ref, individual_ref, download_ref, product_type, product_origin, geoid, logfiles.created, message_id, message_body, message_type FROM products JOIN logfiles ON logfiles.product_id = products.product_id JOIN downloads ON products.download_id = downloads.download_id ' +
      clause +
      ' ORDER BY logfiles.created ASC LIMIT 100;',
    parameters: { type },
  });
};

exports.searchLogsByGeoid = function (ctx, geoid) {
  return slsAuroraClient.query({
    sql:
      'SELECT product_ref, individual_ref, download_ref, product_type, product_origin, geoid, logfiles.created, message_id, message_body, message_type FROM products JOIN logfiles ON logfiles.product_id = products.product_id JOIN downloads ON products.download_id = downloads.download_id WHERE geoid = :geoid ORDER BY logfiles.created ASC LIMIT 100;',
    parameters: { geoid },
  });
};

exports.searchLogsByReference = function (ctx, ref) {
  return slsAuroraClient.query({
    sql:
      'SELECT product_ref, individual_ref, download_ref, product_type, product_origin, geoid, logfiles.created, message_id, message_body, message_type FROM products JOIN logfiles ON logfiles.product_id = products.product_id JOIN downloads ON products.download_id = downloads.download_id WHERE product_ref = :ref OR individual_ref = :ref OR download_ref = :ref ORDER BY logfiles.created ASC LIMIT 100;',
    parameters: { ref },
  });
};
