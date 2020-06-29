// @ts-check

const AWS = require('aws-sdk');
const fs = require('fs');
const stream = require('stream');
const zlib = require('zlib');
const spawn = require('child_process').spawn;
const { log } = require('./logger');
const S3 = new AWS.S3({ apiVersion: '2006-03-01' });

exports.putTextToS3 = function (bucketName, keyName, text, contentType, useGzip) {
  return new Promise((resolve, reject) => {
    const objectParams = {
      Bucket: bucketName,
      Key: keyName,
      Body: text,
      ContentType: contentType,
    };

    if (useGzip) {
      zlib.gzip(text, function (error, result) {
        if (error) {
          return reject(error);
        }
        objectParams.Body = result;
      });

      objectParams.ContentEncoding = 'gzip';
    }

    const uploadPromise = S3.putObject(objectParams).promise();
    uploadPromise
      .then(data => {
        log.info(`Successfully uploaded data to s3://${bucketName}/${keyName}`);
        log.info(data);
        return resolve();
      })
      .catch(err => {
        log.error(err);
        return reject(err);
      });
  });
};

exports.putFileToS3 = function (bucket, key, filePathToUpload, contentType, useGzip) {
  return new Promise((resolve, reject) => {
    log.info(`uploading file to s3 (${filePathToUpload} as s3://${bucket}/${key}), please wait...`);

    const uploadStream = () => {
      const pass = new stream.PassThrough();

      let params = {
        Bucket: bucket,
        Key: key,
        Body: pass,
        ContentType: contentType,
      };

      if (useGzip) {
        params = { ...params, ContentEncoding: 'gzip' };
      }

      return {
        writeStream: pass,
        promise: S3.upload(params).promise(),
      };
    };
    const { writeStream, promise } = uploadStream();
    const readStream = fs.createReadStream(filePathToUpload);
    const z = zlib.createGzip();

    if (useGzip) {
      readStream.pipe(z).pipe(writeStream);
    } else {
      readStream.pipe(writeStream);
    }

    promise
      .then(result => {
        log.info(
          `uploading (${filePathToUpload} as s3://${bucket}/${key}) completed successfully.`,
        );
        log.info('result', result);
        return resolve();
      })
      .catch(err => {
        log.error(`upload (${filePathToUpload} as s3://${bucket}/${key}) failed.`);
        log.error(err);
        return reject(err);
      });
  });
};

exports.getObject = function (bucket, key) {
  return new Promise((resolve, reject) => {
    S3.getObject(
      {
        Bucket: bucket,
        Key: key,
      },
      function (err, data) {
        if (err) {
          return reject(err);
        }

        const body = data.Body;

        if (data.ContentEncoding === 'gzip') {
          // @ts-ignore
          zlib.gunzip(body, function (err, fileBuffer) {
            if (err) {
              return reject(err);
            }
            return resolve(JSON.parse(fileBuffer.toString('utf-8')));
          });
        } else {
          return resolve(JSON.parse(data.Body.toString('utf-8')));
        }
      },
    );
  });
};

exports.s3Sync = async function (currentTilesDir, bucketName, destinationFolder) {
  return new Promise((resolve, reject) => {
    const application = 'aws';
    const args = [
      's3',
      `sync`,
      currentTilesDir,
      `s3://${bucketName}/${destinationFolder}`,
      '--content-encoding',
      'gzip',
    ];
    const command = `${application} ${args.join(' ')}`;
    log.info(`running: ${command}`);

    const proc = spawn(application, args);

    proc.stdout.on('data', data => {
      console.log(data.toString());
    });

    proc.stderr.on('data', data => {
      console.log(data.toString());
    });

    proc.on('error', err => {
      log.error(err);
      reject(err);
    });

    proc.on('close', code => {
      log.info(`completed copying tiles from   ${currentTilesDir} to s3://${bucketName}`);
      resolve({ command });
    });
  });
};

exports.emptyS3Directory = emptyDirectory;

async function emptyDirectory(bucket, dir) {
  const listParams = {
    Bucket: bucket,
    Prefix: dir,
  };

  const listedObjects = await S3.listObjectsV2(listParams).promise();

  if (listedObjects.Contents.length === 0) {
    return;
  }

  const deleteParams = {
    Bucket: bucket,
    Delete: { Objects: [] },
  };

  listedObjects.Contents.forEach(({ Key }) => {
    deleteParams.Delete.Objects.push({ Key });
  });

  const result = await S3.deleteObjects(deleteParams).promise();
  log.info(result);

  if (listedObjects.IsTruncated) {
    await emptyDirectory(bucket, dir);
  }
}

// streams a download to the filesystem.
// optionally un-gzips the file to a new location
exports.streamS3toFileSystem = function (bucket, key, s3DestFile, s3UnzippedFile = null) {
  return new Promise((resolve, reject) => {
    const gunzip = zlib.createGunzip();
    const file = fs.createWriteStream(s3DestFile);

    S3.getObject({ Bucket: bucket, Key: key })
      .on('error', function (err) {
        log.error(err);
        return reject(err);
      })
      .on('httpData', function (chunk) {
        file.write(chunk);
      })
      .on('httpDone', function () {
        file.end();
        console.log('downloaded file to' + s3DestFile);

        if (s3UnzippedFile) {
          fs.createReadStream(s3DestFile)
            .on('error', err => {
              log.error(err);
              return reject(err);
            })
            .on('end', () => {
              console.log('deflated to ' + s3UnzippedFile);
              return resolve();
            })
            .pipe(gunzip)
            .pipe(fs.createWriteStream(s3UnzippedFile));
        }
      })
      .send();
  });
};
