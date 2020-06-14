// @ts-check

const fs = require('fs');
const spawn = require('child_process').spawn;
const secret = require('../mysql.json');
const { directories, buckets } = require('./constants');
const { putFileToS3 } = require('./primitives/s3Operations');

const date = new Date();

const filename = `marhill-main-${secret.device}-${date.toISOString()}.sql`;
const filepath = `${directories.processedDir}/${filename}`;

const wstream = fs.createWriteStream(filepath);

const mysqldump = spawn('mysqldump', [
  '-u',
  secret.connection.user,
  '-p' + secret.connection.password,
  secret.connection.database,
]);

mysqldump.stdout
  .pipe(wstream)
  .on('close', function () {
    console.log('Completed dump.  Starting upload to S3');
    putFileToS3(buckets.marhillMainDB, filename, filepath, 'application/sql', true)
      .then(() => {
        console.log('S3 upload of dump file completed successfully.');
      })
      .catch(err => {
        console.error(err);
        console.error('S3 upload of dump file failed');
      });
  })
  .on('error', function (err) {
    console.log(err);
  });
