const {
  queryWritePageCheck,
  queryHealth,
  queryPage,
  queryWritePage,
  queryHash,
  queryCreateDownloadRecord,
  queryGeographicIdentifier,
  queryCreateProductRecord,
} = require('../primitives/queries');

exports.checkHealth = async function () {
  try {
    await queryHealth();
    return true;
  } catch (e) {
    return false;
  }
};

exports.recordPageCheck = async function (pageId) {
  const query = await queryWritePageCheck(pageId);
  return query.insertId;
};

exports.fetchPageIdIfExists = async function (pageName) {
  const query = await queryPage(pageName);
  console.log(`queryPage: ${pageName}`, query);
  if (query.records.length) {
    return query.records[0].page_id;
  }
  return -1;
};

exports.createPage = async function (pageName) {
  const query = await queryWritePage(pageName);
  console.log(`createPage: ${pageName}`, query);
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
