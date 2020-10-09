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
  createMessageRecord,
} = require('./queries');
const { getSourceType, unwindStack, getTimestamp } = require('./misc');
const { sourceTypes, dispositions, fileFormats, productOrigins } = require('./constants');

exports.DBWrites = async function (
  ctx,
  sourceNameInput,
  computedHash,
  rawKey,
  downloadRef,
  filePath,
  productRef,
  geoid,
  productKey,
  individualRef,
  messagePayload,
) {
  ctx.process.push({ name: 'DBWrites', timestamp: getTimestamp() });

  let sourceLine;
  const transactionId = await startTransaction(ctx);
  ctx.log.info('Transaction started.', { transactionId });

  try {
    let [sourceId, sourceType] = await fetchSourceIdIfExists(ctx, sourceNameInput);

    if (sourceId === -1) {
      ctx.log.info(`Source doesn't exist in database.  Creating a new source.`);
      sourceType = getSourceType(ctx, sourceNameInput);
      sourceId = await createSource(ctx, sourceNameInput, sourceType, transactionId);
      sourceLine = `Created a new source record for: ${sourceNameInput} of type ${sourceType}`;
    } else {
      sourceLine = `Found source record for: ${sourceNameInput}`;
    }

    // todo, accomodate 'received' disposition by prompt
    const checkId = await recordSourceCheck(ctx, sourceId, sourceType, transactionId);

    // if not in database write a record in the download table
    const downloadId = await constructDownloadRecord(
      ctx,
      sourceId,
      checkId,
      computedHash,
      rawKey,
      downloadRef,
      filePath,
      ctx.messageId,
      transactionId,
    );
    ctx.log.info(`Created download record.`, { downloadId });

    await queryCreateProductRecord(
      ctx,
      downloadId,
      productRef,
      individualRef,
      fileFormats.NDGEOJSON.extension,
      productOrigins.ORIGINAL,
      geoid,
      `${productKey}.ndgeojson`,
      ctx.messageId,
      transactionId,
    );
    ctx.log.info('NdGeoJson product record was created.');

    await createMessageRecord(
      ctx,
      ctx.messageId,
      JSON.stringify(messagePayload),
      ctx.type,
      transactionId,
    );
    ctx.log.info('Message reference record was created.');

    await commitTransaction(ctx, transactionId);
    ctx.log.info('Transaction Committed');

    ctx.log.info(sourceLine);
    ctx.log.info(`Recorded source check as disposition: 'viewed'`);
    ctx.log.info(`created record in 'downloads' table.  ref: ${downloadRef}`);
    ctx.log.info(`wrote NDgeoJSON product record.  ref: ${productRef}`);
    unwindStack(ctx, 'DBWrites');
    return downloadId;
  } catch (err) {
    ctx.log.error('Error', { err: err.message, stack: err.stack });
    await rollbackTransaction(ctx, transactionId);
    ctx.log.info('Database transaction has been rolled back.');
    throw new Error('database transaction failed.  re-throwing to roll back S3 saves.');
  }
};

exports.acquireConnection = acquireConnection;

async function acquireConnection(ctx) {
  ctx.process.push({ name: 'acquireConnection', timestamp: getTimestamp() });

  const seconds = 10;
  let attempts = 5;
  let connected = false;

  for (let i = 0; i < attempts; i++) {
    connected = await checkHealth(ctx);
    if (!connected) {
      ctx.log.info(`attempt failed.  trying again in ${seconds} seconds...`);
      await setPause(ctx, seconds * 1000);
    } else {
      break;
    }
  }

  if (!connected) {
    throw new Error('unable to establish database connection');
  }

  unwindStack(ctx, 'acquireConnection');
  ctx.log.info('connection to database confirmed');
}

function setPause(ctx, timer) {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve();
    }, timer);
  });
}

async function checkHealth(ctx) {
  try {
    await queryHealth(ctx);
    return true;
  } catch (e) {
    return false;
  }
}

exports.recordSourceCheck = recordSourceCheck;

async function recordSourceCheck(ctx, sourceId, sourceType, transactionId) {
  ctx.process.push({ name: 'recordSourceCheck', timestamp: getTimestamp() });

  if (sourceType !== sourceTypes.EMAIL && sourceType !== sourceTypes.WEBPAGE) {
    throw new Error('unexpected sourceType');
  }

  const disposition =
    sourceType === sourceTypes.EMAIL ? dispositions.RECEIVED : dispositions.VIEWED;
  const query = await queryWriteSourceCheck(ctx, sourceId, disposition, transactionId);

  unwindStack(ctx, 'recordSourceCheck');
  return query.insertId;
}

exports.fetchSourceIdIfExists = fetchSourceIdIfExists;

async function fetchSourceIdIfExists(ctx, pageName) {
  ctx.process.push({ name: 'fetchSourceIdIfExists', timestamp: getTimestamp() });

  const query = await querySource(ctx, pageName);

  unwindStack(ctx, 'fetchSourceIdIfExists');

  if (query.records.length) {
    return [query.records[0].source_id, query.records[0].source_type];
  }
  return [-1, -1];
}

exports.createSource = createSource;

async function createSource(ctx, sourceName, sourceType, transactionId) {
  ctx.process.push({ name: 'createSource', timestamp: getTimestamp() });

  const query = await queryWriteSource(ctx, sourceName, sourceType, transactionId);
  if (query.numberOfRecordsUpdated === 1) {
    unwindStack(ctx, 'createSource');
    return query.insertId;
  }
  throw new Error(`unable to create page record for: ${sourceName}`);
}

exports.doesHashExist = async function (ctx, computedHash) {
  ctx.process.push({ name: 'doesHashExist', timestamp: getTimestamp() });

  const query = await queryHash(ctx, computedHash);
  if (query.records.length) {
    ctx.log.info('Hash exists in database.  File has already been processed.');
    unwindStack(ctx, 'doesHashExist');
    return true;
  }
  ctx.log.info('Hash is unique.  Processing new download.');
  unwindStack(ctx, 'doesHashExist');
  return false;
};

exports.constructDownloadRecord = constructDownloadRecord;

async function constructDownloadRecord(
  ctx,
  sourceId,
  checkId,
  computedHash,
  rawKey,
  downloadRef,
  filePath,
  messageId,
  transactionId,
) {
  ctx.process.push({ name: 'constructDownloadRecord', timestamp: getTimestamp() });

  const originalFilename = path.parse(filePath).base;

  const query = await queryCreateDownloadRecord(
    ctx,
    sourceId,
    checkId,
    computedHash,
    rawKey,
    downloadRef,
    originalFilename,
    messageId,
    transactionId,
  );

  if (!query || !query.insertId) {
    throw new Error('unexpected result from create download request');
  }

  unwindStack(ctx, 'constructDownloadRecord');
  return query.insertId;
}

exports.makeS3Key = makeS3Key;

function makeS3Key(ctx, str) {
  return str.replace(/[^a-z0-9]+/gi, '-');
}

exports.lookupCleanGeoName = async function (ctx, fipsDetails) {
  ctx.process.push({ name: 'lookupCleanGeoName', timestamp: getTimestamp() });

  const { SUMLEV, STATEFIPS, COUNTYFIPS, PLACEFIPS } = fipsDetails;

  let geoid;

  if (SUMLEV === '040') {
    geoid = STATEFIPS;
  } else if (SUMLEV === '050') {
    geoid = `${STATEFIPS}${COUNTYFIPS}`;
  } else if (SUMLEV === '160') {
    geoid = `${STATEFIPS}${PLACEFIPS}`;
  } else {
    ctx.log.error('SUMLEV out of range');
    throw new Error('SUMLEV out of range');
  }

  const query = await queryGeographicIdentifier(ctx, geoid);
  ctx.log.info('queryResults', { query });

  if (!query || !query.records || !query.records.length) {
    throw new Error(
      `No geographic match found.  SUMLEV:${SUMLEV} STATEFIPS:${STATEFIPS} COUNTYFIPS:${COUNTYFIPS} PLACEFIPS:${PLACEFIPS}`,
    );
  }

  const rawGeoName = query.records[0].geoname;

  ctx.log.info(`Found corresponding geographic area: ${rawGeoName}`);

  // Alter geo name to be s3 key friendly (all non alphanumeric become -)
  const geoName = makeS3Key(ctx, rawGeoName);

  unwindStack(ctx, 'lookupCleanGeoName');
  return { geoid, geoName };
};

exports.getSplittableDownloads = async function (ctx, geoid) {
  ctx.process.push({ name: 'getSplittableDownloads', timestamp: getTimestamp() });

  let query;

  if (geoid) {
    query = await queryAllOriginalRecentDownloadsWithGeoid(ctx, geoid);
  } else {
    query = await queryAllOriginalRecentDownloads(ctx);
  }

  if (!query) {
    throw new Error(`Problem running getSplittableDownlods Query`);
  }

  unwindStack(ctx, 'getSplittableDownloads');
  return { rows: query.records };
};

exports.getCountiesByState = async function (ctx, geoid) {
  ctx.process.push({ name: 'getCountiesByState', timestamp: getTimestamp() });

  const query = await queryAllCountiesFromState(ctx, geoid);
  if (!query) {
    throw new Error(`Problem running queryAllCountiesFromState Query`);
  }

  unwindStack(ctx, 'getCountiesByState');
  return query.records;
};

exports.querySourceNameExact = async function (ctx, sourceName) {
  ctx.process.push({ name: 'querySourceNameExact', timestamp: getTimestamp() });

  const query = await querySource(ctx, sourceName);
  if (!query) {
    throw new Error(`Problem running querySourceNameExact Query`);
  }
  unwindStack(ctx, 'querySourceNameExact');
  return query.records;
};

exports.querySourceNames = async function (ctx, sourceName) {
  ctx.process.push({ name: 'querySourceNames', timestamp: getTimestamp() });

  // take root of http source, or domain of email sources

  // figure if email or webpage
  let query;

  if (sourceName.includes('@')) {
    const domain = sourceName.split('@');
    query = await querySourceNamesLike(ctx, domain[1]);
  } else if (
    sourceName.includes('http://') ||
    sourceName.includes('https://') ||
    sourceName.includes('ftp://') ||
    sourceName.includes('ftps://')
  ) {
    const root = url.parse(sourceName).hostname;
    query = await querySourceNamesLike(ctx, root);
  } else {
    throw new Error('invalid sourceName input in querySourceNames');
  }

  if (!query) {
    throw new Error(`Problem running querySourceNames Query`);
  }

  unwindStack(ctx, 'querySourceNames');
  return query.records;
};

exports.initiateDatabaseHeartbeat = function (ctx, seconds) {
  ctx.process.push({ name: 'initiateDatabaseHeartbeat', timestamp: getTimestamp() });

  let interval = setInterval(() => {
    // meant to be non-blocking
    ctx.log.info('database keepalive initiated...');
    acquireConnection(ctx);
  }, seconds * 1000);

  unwindStack(ctx, 'initiateDatabaseHeartbeat');
  return interval;
};
