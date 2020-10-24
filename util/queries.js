// @ts-check
const config = require('config');
const { unwindStack, getTimestamp } = require('./misc');

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
  messageId,
  transactionId,
) {
  // write a download record to unique identify a downloaded file.
  return slsAuroraClient.query({
    sql:
      'INSERT INTO downloads(source_id, check_id, checksum, raw_key, message_id, download_ref, original_filename) VALUES (:sourceId, :checkId, :checksum, :rawKey, :messageId, :downloadRef, :originalFilename);',
    parameters: { sourceId, checkId, checksum, rawKey, messageId, downloadRef, originalFilename },
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
  messageId,
  transactionId,
) {
  ctx.process.push({ name: 'queryCreateProductRecord', timestamp: getTimestamp() });

  const query = await slsAuroraClient.query({
    sql:
      'INSERT INTO products(download_id, product_ref, individual_ref, product_type, product_origin, geoid, product_key, message_id) VALUES (:downloadId, :productRef, :individualRef, :productType, :productOrigin, :geoid, :productKey, :messageId);',
    parameters: {
      downloadId,
      productRef,
      individualRef,
      productType,
      productOrigin,
      geoid,
      productKey,
      messageId,
    },
    transactionId,
  });

  ctx.log.info('product record created', { record: query });
  const product_id = query.insertId;
  ctx.log.info('ProductId of created record: ' + product_id);

  unwindStack(ctx, 'queryCreateProductRecord');
  return product_id;
};

exports.createMessageRecord = function (
  ctx,
  messageId,
  messagePayload,
  messageType,
  transactionId,
) {
  return slsAuroraClient.query({
    sql:
      'INSERT INTO messages(message_id, message_body, message_type) VALUES (:messageId, :messagePayload, :messageType);',
    parameters: {
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

exports.queryAllCountySubdivisionsFromState = function (ctx, geoid) {
  return slsAuroraClient.query({
    sql: `SELECT geoid, geoname FROM geographic_identifiers WHERE LEFT(geoid, 2) = :geoid and sumlev = "060" order by geoname asc;`,
    parameters: {
      geoid,
    },
  });
};

exports.queryAllOriginalRecentDownloads = function (ctx) {
  return slsAuroraClient.query(
    'select geographic_identifiers.geoid, geoname, source_name, source_type, downloads.download_id, download_ref, product_id, product_ref, last_checked, product_key, original_filename  from downloads left join products on products.download_id = downloads.download_id join source_checks on source_checks.check_id = downloads.check_id join sources on sources.source_id = downloads.source_id join geographic_identifiers on geographic_identifiers.geoid = products.geoid where products.product_type = "ndgeojson" and products.product_origin="original" AND geographic_identifiers.sumlev = "040" ORDER BY products.created DESC LIMIT 30',
  );
};

exports.queryAllOriginalRecentDownloadsWithGeoid = function (ctx, geoid) {
  return slsAuroraClient.query({
    sql:
      'select geographic_identifiers.geoid, geoname, source_name, source_type, downloads.download_id, download_ref, product_id, product_ref, last_checked, product_key, original_filename  from downloads left join products on products.download_id = downloads.download_id join source_checks on source_checks.check_id = downloads.check_id join sources on sources.source_id = downloads.source_id join geographic_identifiers on geographic_identifiers.geoid = products.geoid where products.product_type = "ndgeojson" and products.product_origin="original" and geographic_identifiers.geoid = :geoid  AND geographic_identifiers.sumlev = "040" ORDER BY products.created DESC LIMIT 30',
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
  ctx.process.push({ name: 'checkForProduct', timestamp: getTimestamp() });
  const query = await slsAuroraClient.query({
    sql:
      'select * from products where products.geoid = :geoid and products.download_id = :downloadId and products.product_type = :format',
    parameters: { geoid, downloadId, format },
  });
  unwindStack(ctx, 'checkForProduct');
  return query.records.length > 0;
};

exports.checkForProducts = async function (ctx, geoid, downloadId) {
  ctx.process.push({ name: 'checkForProducts', timestamp: getTimestamp() });
  const query = await slsAuroraClient.query({
    sql:
      'select * from products where products.geoid = :geoid and products.download_id = :downloadId',
    parameters: { geoid, downloadId },
  });
  unwindStack(ctx, 'checkForProducts');
  return query.records;
};

exports.searchLogsByType = function (ctx, type) {
  let clause = '';
  if (type !== 'all') {
    clause = 'WHERE message_type = :type';
  }
  return slsAuroraClient.query({
    sql:
      'SELECT product_ref, individual_ref, download_ref, product_type, product_origin, geoid, messages.created, messages.message_id, message_body, message_type FROM products JOIN messages ON messages.message_id = products.message_id JOIN downloads ON products.download_id = downloads.download_id ' +
      clause +
      ' ORDER BY messages.created DESC LIMIT 100;',
    parameters: { type },
  });
};

exports.searchLogsByGeoid = function (ctx, geoid) {
  return slsAuroraClient.query({
    sql:
      'SELECT product_ref, individual_ref, download_ref, product_type, product_origin, geoid, messages.created, messages.message_id, message_body, message_type FROM products JOIN messages ON messages.message_id = products.message_id JOIN downloads ON products.download_id = downloads.download_id WHERE geoid = :geoid ORDER BY messages.created DESC LIMIT 100;',
    parameters: { geoid },
  });
};

exports.searchLogsByReference = function (ctx, ref) {
  return slsAuroraClient.query({
    sql:
      'SELECT product_ref, individual_ref, download_ref, product_type, product_origin, geoid, messages.created, messages.message_id, message_body, message_type FROM products JOIN messages ON messages.message_id = products.message_id JOIN downloads ON products.download_id = downloads.download_id WHERE product_ref = :ref OR individual_ref = :ref OR download_ref = :ref ORDER BY messages.created DESC LIMIT 100;',
    parameters: { ref },
  });
};

exports.getDownloadsData = async function (ctx) {
  ctx.process.push({ name: 'getDownloadsData', timestamp: getTimestamp() });
  const query = await slsAuroraClient.query({
    sql: 'SELECT download_id, source_id, created, download_ref, raw_key FROM downloads',
  });
  unwindStack(ctx, 'getDownloadsData');
  return query.records;
};

exports.getSourceData = async function (ctx) {
  ctx.process.push({ name: 'getSourceData', timestamp: getTimestamp() });
  const query = await slsAuroraClient.query({
    sql:
      'SELECT sources.source_id, sources.source_name, sources.source_type, source_checks.last_checked, source_checks.disposition FROM sources join source_checks ON sources.source_id = source_checks.source_id',
  });
  unwindStack(ctx, 'getSourceData');
  return query.records;
};

exports.getProductsData = async function (ctx, fips) {
  ctx.process.push({ name: 'getProductsData', timestamp: getTimestamp() });
  const query = await slsAuroraClient.query({
    sql:
      'SELECT product_id, product_ref, individual_ref, product_type, product_origin, geoid, product_key, download_id FROM products WHERE geoid LIKE :fips',
    parameters: { fips: `${fips}%` },
  });
  unwindStack(ctx, 'getProductsData');
  return query.records;
};

exports.getGeoIdentifiersData = async function (ctx) {
  ctx.process.push({ name: 'getGeoIdentifiersData', timestamp: getTimestamp() });
  const query = await slsAuroraClient.query({
    sql:
      'SELECT geoid, geoname, sumlev FROM geographic_identifiers WHERE geoid IN (SELECT DISTINCT geoid FROM products)',
  });
  unwindStack(ctx, 'getGeoIdentifiersData');
  return query.records;
};

exports.getSQSMessagesByGeoidAndType = async function (ctx, messageType, geoid) {
  ctx.process.push({ name: 'getSQSMessagesByGeoidAndType', timestamp: getTimestamp() });
  const query = await slsAuroraClient.query({
    sql:
      'SELECT messages.created, messages.message_id, messages.message_type, messages.message_body, products.geoid FROM messages JOIN products ON messages.message_id = products.message_id WHERE message_type=:messageType AND geoid LIKE :geoid ORDER BY messages.created DESC LIMIT 300',
    parameters: { messageType, geoid: `${geoid}%` },
  });
  unwindStack(ctx, 'getSQSMessagesByGeoidAndType');
  return query.records;
};

exports.queryProductByIndividualRef = async function (ctx, individualRef) {
  ctx.process.push({ name: 'queryProductByIndividualRef', timestamp: getTimestamp() });
  const query = await slsAuroraClient.query({
    sql: 'SELECT * from products where individual_ref=:individualRef',
    parameters: { individualRef },
  });
  unwindStack(ctx, 'queryProductByIndividualRef');
  return query.records;
};

exports.queryProductsByProductRef = async function (ctx, productRef) {
  ctx.process.push({ name: 'queryProductsByProductRef', timestamp: getTimestamp() });
  const query = await slsAuroraClient.query({
    sql: 'SELECT * from products where product_ref=:productRef',
    parameters: { productRef },
  });
  unwindStack(ctx, 'queryProductsByProductRef');
  return query.records;
};

exports.getProductsByDownloadRef = async function (ctx, downloadRef) {
  ctx.process.push({ name: 'getProductsByDownloadRef', timestamp: getTimestamp() });
  const query = await slsAuroraClient.query({
    sql:
      'SELECT * from products JOIN downloads on products.download_id = downloads.download_id where download_ref=:downloadRef',
    parameters: { downloadRef },
  });
  unwindStack(ctx, 'getProductsByDownloadRef');
  return query.records;
};

exports.getDownloadsByDownloadRef = async function (ctx, downloadRef) {
  ctx.process.push({ name: 'getDownloadsByDownloadRef', timestamp: getTimestamp() });
  const query = await slsAuroraClient.query({
    sql: 'SELECT * from downloads where download_ref=:downloadRef',
    parameters: { downloadRef },
  });
  unwindStack(ctx, 'getDownloadsByDownloadRef');
  return query.records;
};

exports.deleteRecordById = async function (ctx, table, idName, value) {
  ctx.process.push({ name: 'deleteRecordById', timestamp: getTimestamp() });
  let query = await slsAuroraClient.query(`DELETE FROM ::table WHERE ::fieldName = :id`, {
    table,
    fieldName: idName,
    id: value,
  });
  let response;
  try {
    if (query.numberOfRecordsUpdated > 0) {
      response = query;
    } else {
      response = 'Could not find record id.  This may be expected';
    }
  } catch (err) {
    ctx.log.error({ err: err.message, stack: err.stack });
    response = 'There was an error a database row.';
  }
  unwindStack(ctx, 'deleteRecordById');
  return response;
};
