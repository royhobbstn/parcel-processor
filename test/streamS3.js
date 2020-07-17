const { streamS3toFileSystem } = require('../util/s3Operations');
const config = require('config');

async function main() {
  await streamS3toFileSystem(
    { log: console, process: [] },
    config.get('Buckets.productsBucket'),
    `06-California/000-California/3edba5c9-b10acc2e-e8b9f70a-06-California.ndgeojson`,
    'test/fileV12.nd.gz',
    'test/fileV12.nd',
  );
  console.log('done');
}

main().catch(err => {
  console.log(err);
});
