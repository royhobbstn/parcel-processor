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

exports.queryPage = function (pageName) {
  // queries if a page exists in the page table
  return slsAuroraClient.query('SELECT * FROM pages WHERE page_url = :pageName;', { pageName });
};

exports.queryWritePage = function (pageName) {
  // writes a new page record in the page table
  return slsAuroraClient.query('INSERT INTO pages(page_url) VALUES (:pageName);', { pageName });
};

exports.queryWritePageCheck = function (pageId) {
  // write a 'pageCheck' record to the database
  // it's a record that a page was checked for a more recent download version
  // it is written whether or not a more recent version was found
  // it is meant to give some guidance as to whether or not to check for
  // a more recent version
  return slsAuroraClient.query('INSERT INTO page_checks(page_id) VALUES (:pageId);', { pageId });
};

exports.queryCreateDownloadRecord = function (pageId, checkId, checksum) {
  // write a download record to unique identify a downloaded file.
  return slsAuroraClient.query(
    'INSERT INTO downloads(page_id, check_id, checksum) VALUES (:pageId, :checkId, :checksum);',
    { pageId, checkId, checksum },
  );
};
