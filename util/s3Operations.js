// @ts-check

const AWS = require('aws-sdk');
const fs = require('fs');
const stream = require('stream');
const zlib = require('zlib');
const spawn = require('child_process').spawn;
const S3 = new AWS.S3({ apiVersion: '2006-03-01', region: 'us-east-2' });
const { unwindStack } = require('./misc');

exports.putTextToS3 = function (ctx, bucketName, keyName, text, contentType, useGzip) {
  ctx.process.push('putTextToS3');

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
        unwindStack(ctx.process, 'putTextToS3');
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
  ctx.process.push('putFileToS3');

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
        unwindStack(ctx.process, 'putFileToS3');
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
  ctx.process.push('getObject');

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
              unwindStack(ctx.process, 'getObject');
              return resolve(fileBuffer.toString('utf-8'));
            }
            unwindStack(ctx.process, 'getObject');
            return resolve(JSON.parse(fileBuffer.toString('utf-8')));
          });
        } else {
          if (data.ContentType === 'text/plain') {
            unwindStack(ctx.process, 'getObject');
            return resolve(data.Body.toString('utf-8'));
          }
          unwindStack(ctx.process, 'getObject');
          return resolve(JSON.parse(data.Body.toString('utf-8')));
        }
      },
    );
  });
};

exports.s3Sync = async function (ctx, currentTilesDir, bucketName, destinationFolder) {
  ctx.process.push('s3Sync');

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
      console.log(data.toString());
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
      unwindStack(ctx.process, 's3Sync');
      resolve({ command });
    });
  });
};

exports.emptyS3Directory = emptyDirectory;

async function emptyDirectory(ctx, bucket, dir) {
  ctx.process.push('emptyDirectory');

  const listParams = {
    Bucket: bucket,
    Prefix: dir,
  };

  const listedObjects = await S3.listObjectsV2(listParams).promise();

  if (listedObjects.Contents.length === 0) {
    unwindStack(ctx.process, 'emptyDirectory');
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

  unwindStack(ctx.process, 'emptyDirectory');
}

// streams a download to the filesystem.
// optionally un-gzips the file to a new location
exports.streamS3toFileSystem = function (ctx, bucket, key, s3DestFile, s3UnzippedFile = null) {
  ctx.process.push('streamS3toFileSystem');

  return new Promise((resolve, reject) => {
    const gunzip = zlib.createGunzip();
    const file = fs.createWriteStream(s3DestFile);

    file.on('finish', () => {
      ctx.log.info('okay, for reals done writing to ' + s3DestFile);

      if (s3UnzippedFile) {
        fs.createReadStream(s3DestFile)
          .on('error', err => {
            ctx.log.warn('Error', { err: err.message, stack: err.stack });
            // for whatever reason, I get a :
            // 'RangeError [ERR_INVALID_OPT_VALUE]: The value "3340558817" is invalid for option "size"
            // Everything still works /shrug
            // not going to reject(err)
          })
          .on('end', () => {
            ctx.log.info('finished defalate read.');
          })
          .pipe(gunzip)
          .pipe(fs.createWriteStream(s3UnzippedFile))
          .on('error', err => {
            ctx.log.warn('error gunziping to write stream');
            return reject(err);
          })
          .on('finish', () => {
            ctx.log.info('finished defalate write to ' + s3UnzippedFile);
            unwindStack(ctx.process, 'streamS3toFileSystem');
            return resolve();
          });
      } else {
        unwindStack(ctx.process, 'streamS3toFileSystem');
      }
    });

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
        ctx.log.info('done reading from S3: ' + key);
        ctx.log.info('waiting for write buffer to complete.');
      })
      .send();
  });
};
