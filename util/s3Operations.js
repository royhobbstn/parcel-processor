// @ts-check

const AWS = require('aws-sdk');
const fs = require('fs');
const stream = require('stream');
const zlib = require('zlib');
const spawn = require('child_process').spawn;
const S3 = new AWS.S3({ apiVersion: '2006-03-01' });

exports.putTextToS3 = function (ctx, bucketName, keyName, text, contentType, useGzip) {
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
        ctx.log.info(`Successfully uploaded data to s3://${bucketName}/${keyName}`);
        ctx.log.info(data);
        return resolve();
      })
      .catch(err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        return reject(err);
      });
  });
};

exports.putFileToS3 = function (ctx, bucket, key, filePathToUpload, contentType, useGzip) {
  return new Promise((resolve, reject) => {
    ctx.log.info(
      `uploading file to s3 (${filePathToUpload} as s3://${bucket}/${key}), please wait...`,
    );

    const uploadStream = ctx => {
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
    const { writeStream, promise } = uploadStream(ctx);
    const readStream = fs.createReadStream(filePathToUpload);
    const z = zlib.createGzip();

    if (useGzip) {
      readStream.pipe(z).pipe(writeStream);
    } else {
      readStream.pipe(writeStream);
    }

    promise
      .then(result => {
        ctx.log.info(
          `uploading (${filePathToUpload} as s3://${bucket}/${key}) completed successfully.`,
        );
        ctx.log.info('result', result);
        return resolve();
      })
      .catch(err => {
        ctx.log.error(`upload (${filePathToUpload} as s3://${bucket}/${key}) failed.`, {
          err: err.message,
          stack: err.stack,
        });
        return reject(err);
      });
  });
};

exports.getObject = function (ctx, bucket, key) {
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

exports.s3Sync = async function (ctx, currentTilesDir, bucketName, destinationFolder) {
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
    ctx.log.info(`running: ${command}`);

    const proc = spawn(application, args);

    proc.stdout.on('data', data => {
      ctx.log.info(data.toString());
    });

    proc.stderr.on('data', data => {
      console.log(data.toString());
    });

    proc.on('error', err => {
      ctx.log.error('Error', { err: err.message, stack: err.stack });
      reject(err);
    });

    proc.on('close', code => {
      ctx.log.info(`completed copying tiles from   ${currentTilesDir} to s3://${bucketName}`);
      resolve({ command });
    });
  });
};

exports.emptyS3Directory = emptyDirectory;

async function emptyDirectory(ctx, bucket, dir) {
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
  ctx.log.info(result);

  if (listedObjects.IsTruncated) {
    await emptyDirectory(ctx, bucket, dir);
  }
}

// streams a download to the filesystem.
// optionally un-gzips the file to a new location
exports.streamS3toFileSystem = function (ctx, bucket, key, s3DestFile, s3UnzippedFile = null) {
  return new Promise((resolve, reject) => {
    const gunzip = zlib.createGunzip();
    const file = fs.createWriteStream(s3DestFile);

    S3.getObject({ Bucket: bucket, Key: key })
      .on('error', function (err) {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('httpData', function (chunk) {
        file.write(chunk);
      })
      .on('httpDone', function () {
        file.end();
        ctx.log.info('downloaded file to' + s3DestFile);

        if (s3UnzippedFile) {
          fs.createReadStream(s3DestFile)
            .on('error', err => {
              ctx.log.error('Error', { err: err.message, stack: err.stack });
              return reject(err);
            })
            .on('end', () => {
              ctx.log.info('deflated to ' + s3UnzippedFile);
              return resolve();
            })
            .pipe(gunzip)
            .pipe(fs.createWriteStream(s3UnzippedFile));
        }
      })
      .send();
  });
};
