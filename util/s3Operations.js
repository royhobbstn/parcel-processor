const AWS = require('aws-sdk');
const fs = require('fs');
const stream = require('stream');
const zlib = require('zlib');

exports.putTextToS3 = function (bucketName, keyName) {
  // not used.  update to streaming and content type & encoding options if that changes.
  return new Promise((resolve, reject) => {
    const objectParams = { Bucket: bucketName, Key: keyName, Body: 'Hello World!' };
    const uploadPromise = new AWS.S3({ apiVersion: '2006-03-01' })
      .putObject(objectParams)
      .promise();
    uploadPromise
      .then(data => {
        console.log(`Successfully uploaded data to s3://${bucketName}/${keyName}`);
        console.log(data);
        resolve();
      })
      .catch(err => {
        console.error(err);
        reject(err);
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
        resolve();
      })
      .catch(err => {
        console.error(`upload (${filePathToUpload} as s3://${bucket}/${key}) failed.`);
        console.error(err);
        reject(err);
      });
  });
};
