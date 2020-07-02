const config = require('config');

const slsAuroraClient = require('data-api-client')({
  secretArn: config.get('RDS.secretArn'),
  resourceArn: config.get('RDS.resourceArn'),
  database: config.get('RDS.database'),
  region: config.get('RDS.region'),
});

function queryHealth() {
  return slsAuroraClient.query(`SELECT 1 + 1;`);
}

async function truncateTable(table) {
  const query = await slsAuroraClient.query(`DELETE FROM ${table};`);
  console.log(query);
}

// ping the database to make sure its up / get it ready
// after that, keep-alives from data-api-client should do the rest
async function init() {
  const seconds = 10;
  let connected = false;
  do {
    console.log('attempting to connect to database');
    connected = await checkHealth();
    if (!connected) {
      console.log(`attempt failed.  trying again in ${seconds} seconds...`);
      await setPause(seconds * 1000);
    }
  } while (!connected);

  console.log('connected');
  await truncateTable('products');
  await truncateTable('downloads');
  await truncateTable('source_checks');
  await truncateTable('sources');

  console.log('done');
}

function setPause(timer) {
  return new Promise(resolve => {
    setTimeout(() => {
      return resolve();
    }, timer);
  });
}

async function checkHealth() {
  try {
    await queryHealth();
    return true;
  } catch (e) {
    return false;
  }
}

init().catch(err => {
  console.error('unexpected error');
  console.error(err);
});
