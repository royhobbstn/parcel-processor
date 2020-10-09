// @ts-check

const AWS = require('aws-sdk');
const fs = require('fs');
const stream = require('stream');
const zlib = require('zlib');
const spawn = require('child_process').spawn;
const S3 = new AWS.S3({ apiVersion: '2006-03-01', region: 'us-east-2' });
const { unwindStack, getTimestamp } = require('./misc');

exports.putTextToS3 = function (ctx, bucketName, keyName, text, contentType, useGzip) {
  ctx.process.push({ name: 'putTextToS3', timestamp: getTimestamp() });

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
        ctx.log.info('upload response', { data });
        unwindStack(ctx, 'putTextToS3');
        return resolve();
      })
      .catch(err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        return reject(err);
      });
  });
};

exports.putFileToS3 = function (
  ctx,
  bucket,
  key,
  filePathToUpload,
  contentType,
  useGzip,
  contentDisposition,
) {
  ctx.process.push({ name: 'putFileToS3', timestamp: getTimestamp() });

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
        params.ContentEncoding = 'gzip';
      }

      if (contentDisposition) {
        params.ContentDisposition = contentDisposition;
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
        unwindStack(ctx, 'putFileToS3');
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
  ctx.process.push({ name: 'getObject', timestamp: getTimestamp() });

  ctx.log.info('getting s3 object', { bucket, key });
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
        ctx.log.info('ContentType: ', { ContentType: data.ContentType });
        ctx.log.info('ContentEncoding: ', { ContentEncoding: data.ContentEncoding });

        if (data.ContentEncoding === 'gzip') {
          // @ts-ignore
          zlib.gunzip(body, function (err, fileBuffer) {
            if (err) {
              return reject(err);
            }
            if (data.ContentType === 'text/plain') {
              unwindStack(ctx, 'getObject');
              return resolve(fileBuffer.toString('utf-8'));
            }
            unwindStack(ctx, 'getObject');
            return resolve(JSON.parse(fileBuffer.toString('utf-8')));
          });
        } else {
          if (data.ContentType === 'text/plain') {
            unwindStack(ctx, 'getObject');
            return resolve(data.Body.toString('utf-8'));
          }
          unwindStack(ctx, 'getObject');
          return resolve(JSON.parse(data.Body.toString('utf-8')));
        }
      },
    );
  });
};

exports.s3Sync = async function (ctx, currentTilesDir, bucketName, destinationFolder) {
  ctx.process.push({ name: 's3Sync', timestamp: getTimestamp() });

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
      // mostly noise
      // console.log(data.toString());
    });

    proc.stderr.on('data', data => {
      // mostly noise
      // console.log(data.toString());
    });

    proc.on('error', err => {
      ctx.log.error('Error', { err: err.message, stack: err.stack });
      reject(err);
    });

    proc.on('close', code => {
      ctx.log.info(`completed copying tiles from   ${currentTilesDir} to s3://${bucketName}`);
      unwindStack(ctx, 's3Sync');
      resolve({ command });
    });
  });
};

exports.emptyS3Directory = emptyDirectory;

async function emptyDirectory(ctx, bucket, dir) {
  ctx.process.push({ name: 'emptyDirectory', timestamp: getTimestamp() });

  const listParams = {
    Bucket: bucket,
    Prefix: dir,
  };

  const listedObjects = await S3.listObjectsV2(listParams).promise();

  if (listedObjects.Contents.length === 0) {
    unwindStack(ctx, 'emptyDirectory');
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
  ctx.log.info(`deleted directory. Bucket: ${bucket} Dir: ${dir}`, {
    deletions: result.Deleted.length,
    errors: result.Errors,
  });

  if (listedObjects.IsTruncated) {
    await emptyDirectory(ctx, bucket, dir);
  }

  unwindStack(ctx, 'emptyDirectory');
}

// streams a download to the filesystem.
// optionally un-gzips the file to a new location
exports.streamS3toFileSystem = async function (
  ctx,
  bucket,
  key,
  s3DestFile,
  s3UnzippedFile = null,
) {
  ctx.process.push({ name: 'streamS3toFileSystem', timestamp: getTimestamp() });

  await new Promise((resolve, reject) => {
    const outputFile = s3UnzippedFile || s3DestFile;
    const writeStream = fs.createWriteStream(outputFile);
    const readStream = S3.getObject({ Bucket: bucket, Key: key }).createReadStream();

    readStream.on('error', err => {
      ctx.log.error('readStream error');
      reject(err);
    });
    readStream.on('finish', () => {
      ctx.log.info('readStream finish');
      ctx.log.info('done reading from S3: ' + key);
      ctx.log.info('waiting for write buffer to complete.');
    });
    writeStream.on('error', err => {
      ctx.log.error('writeStream error');
      reject(err);
    });
    writeStream.on('finish', () => {
      ctx.log.info('writeStream finish');
      ctx.log.info('done writing S3 file to ' + outputFile);
      unwindStack(ctx, 'streamS3toFileSystem');
      resolve();
    });
    if (s3UnzippedFile) {
      const gunzip = zlib.createGunzip();
      gunzip.on('error', err => {
        ctx.log.error('gunzip error');
        reject(err);
      });
      gunzip.on('finish', () => {
        ctx.log.info('gunzip finish');
      });
      readStream.pipe(gunzip).pipe(writeStream);
    } else {
      readStream.pipe(writeStream);
    }
  });
};
