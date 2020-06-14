const mysql = require('mysql2/promise');
const secret = require('../mysql.json');

let connection;

init()
  .catch(err => {
    console.error('unexpected error');
    console.error(err);
  })
  .finally(async () => {
    await connection.end();
  });

async function init() {
  connection = await mysql.createConnection(secret.connection);

  const [rows] = await connection.execute(`SELECT 1 + 1;`);
  console.log(rows[0]);

  console.log('connected');

  await truncateTable('products');
  await truncateTable('downloads');
  await truncateTable('source_checks');
  await truncateTable('sources');

  console.log('done');
}

async function truncateTable(tbl) {
  // wth, why cant escape tbl?
  const resultSet = await connection.execute(`DELETE FROM ` + tbl);
  console.log(`Deleted ${resultSet[0].affectedRows} records from ${tbl}`);
}
