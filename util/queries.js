const config = require('config');

const slsAuroraClient = require('data-api-client')({
  secretArn: config.get('secretArn'),
  resourceArn: config.get('resourceArn'),
  database: config.get('database'),
  region: config.get('region'),
});

exports.queryHash = function () {
  // queries the database for a given hash
  // (if found, than we already have the most recent download)
  return slsAuroraClient.query(`SELECT * FROM pages`); // TODO change query
};

exports.writePageCheck = function () {
  // write a 'pageCheck' record to the database
  // it's a record that a page was checked for a more recent download version
  // it is written whether or not a more recent version was found
  // it is meant to give some guidance as to whether or not to check for
  // a more recent version
};
