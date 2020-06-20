const AWS = require('aws-sdk');
const RDS = new AWS.RDSDataService();

const slsAuroraClient = require('data-api-client')({
  secretArn:
    'arn:aws:secretsmanager:us-east-2:000009394762:secret:rds-db-credentials/cluster-DFMKRGAEZALSW5CFHLUBKWDLKQ/admin-PnSVCT',
  resourceArn: 'arn:aws:rds:us-east-2:000009394762:cluster:parcel-outlet-dev',
  database: 'main',
  region: 'us-east-2',
});

main();

async function main() {
  const response = await slsAuroraClient.query(`SELECT 1 + 1;`);
  console.log(response);
}
