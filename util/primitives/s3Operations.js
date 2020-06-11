// @ts-check

const AWS = require('aws-sdk');
const fs = require('fs');
const stream = require('stream');
const zlib = require('zlib');
const spawn = require('child_process').spawn;

exports.putTextToS3 = function (bucketName, keyName, text, contentType) {
  return new Promise((resolve, reject) => {
    const objectParams = {
      Bucket: bucketName,
      Key: keyName,
      Body: text,
      ContentType: contentType,
    };
    const uploadPromise = new AWS.S3({ apiVersion: '2006-03-01' })
      .putObject(objectParams)
      .promise();
    uploadPromise
      .then(data => {
        console.log(`Successfully uploaded data to s3://${bucketName}/${keyName}`);
        console.log(data);
        return resolve();
      })
      .catch(err => {
        console.error(err);
        return reject(err);
      });
  });
};

exports.putFileToS3 = function (bucket, key, filePathToUpload, contentType, useGzip) {
  return new Promise((resolve, reject) => {
    console.log(
      `uploading file to s3 (${filePathToUpload} as s3://${bucket}/${key}), please wait...`,
    );

    const uploadStream = () => {
      const s3 = new AWS.S3();
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
        promise: s3.upload(params).promise(),
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
        console.log(
          `uploading (${filePathToUpload} as s3://${bucket}/${key}) completed successfully.`,
        );
        console.log(result);
        return resolve();
      })
      .catch(err => {
        console.error(`upload (${filePathToUpload} as s3://${bucket}/${key}) failed.`);
        console.error(err);
        return reject(err);
      });
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
    console.log(`running: ${command}`);

    const proc = spawn(application, args);

    proc.stdout.on('data', data => {
      console.log(`stdout: ${data.toString()}`);
    });

    proc.stderr.on('data', data => {
      console.log(data.toString());
    });

    proc.on('error', err => {
      console.error(err);
      reject(err);
    });

    proc.on('close', code => {
      console.log(`completed copying tiles from   ${currentTilesDir} to s3://${bucketName}`);
      resolve({ command });
    });
  });
};

exports.emptyS3Directory = emptyDirectory;

async function emptyDirectory(bucket, dir) {
  const s3 = new AWS.S3();

  const listParams = {
    Bucket: bucket,
    Prefix: dir,
  };

  const listedObjects = await s3.listObjectsV2(listParams).promise();

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

  const result = await s3.deleteObjects(deleteParams).promise();
  console.log(result);

  if (listedObjects.IsTruncated) {
    await emptyDirectory(bucket, dir);
  }
}
