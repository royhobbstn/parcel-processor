// @ts-check

const AWS = require('aws-sdk');
const path = require('path');
const { buckets, s3deleteType } = require('../constants');
const { putFileToS3, emptyS3Directory } = require('../primitives/s3Operations');
const { lookupState } = require('../lookupState');

exports.uploadRawFileToS3 = async function (filePath, rawKey) {
  const extension = path.extname(rawKey);

  if (extension !== '.zip') {
    throw new Error('expected filename extension to be .zip');
  }

  // save downloaded file to the raw bucket in s3 (don't gzip here)
  await putFileToS3(buckets.rawBucket, rawKey, filePath, 'application/zip', false);
};

exports.uploadProductFiles = async function (key, outputPath) {
  const statFile = putFileToS3(
    buckets.productsBucket,
    `${key}-stat.json`,
    `${outputPath}.json`,
    'application/json',
    true,
  );
  const ndgeojsonFile = putFileToS3(
    buckets.productsBucket,
    `${key}.ndgeojson`,
    `${outputPath}.ndgeojson`,
    'application/geo+json-seq',
    true,
  );

  await Promise.all([statFile, ndgeojsonFile]);
  console.log('Output files were successfully loaded to S3');

  return;
};

exports.createRawDownloadKey = function (fipsDetails, geoid, geoName, downloadRef) {
  const stateName = lookupState(fipsDetails.STATEFIPS).replace(/[^a-z0-9]+/gi, '-');

  let key;

  if (fipsDetails.SUMLEV === '040') {
    key = `${fipsDetails.STATEFIPS}-${stateName}/${fipsDetails.COUNTYFIPS}-${geoName}/${downloadRef}-${geoid}-${geoName}`;
  } else if (fipsDetails.SUMLEV === '050') {
    key = `${fipsDetails.STATEFIPS}-${stateName}/${fipsDetails.COUNTYFIPS}-${geoName}/${downloadRef}-${geoid}-${geoName}-${stateName}`;
  } else if (fipsDetails.SUMLEV === '160') {
    key = `${fipsDetails.STATEFIPS}-${stateName}/${fipsDetails.PLACEFIPS}-${geoName}/${downloadRef}-${geoid}-${geoName}-${stateName}`;
  } else {
    throw new Error('unexpected sumlev.');
  }

  return `${key}.zip`;
};

exports.createProductDownloadKey = function (fipsDetails, geoid, geoName, downloadRef, productRef) {
  const stateName = lookupState(fipsDetails.STATEFIPS).replace(/[^a-z0-9]+/gi, '-');

  let key;

  if (fipsDetails.SUMLEV === '040') {
    key = `${fipsDetails.STATEFIPS}-${stateName}/${fipsDetails.COUNTYFIPS}-${geoName}/${downloadRef}-${productRef}-${geoid}-${geoName}`;
  } else if (fipsDetails.SUMLEV === '050') {
    key = `${fipsDetails.STATEFIPS}-${stateName}/${fipsDetails.COUNTYFIPS}-${geoName}/${downloadRef}-${productRef}-${geoid}-${geoName}-${stateName}`;
  } else if (fipsDetails.SUMLEV === '160') {
    key = `${fipsDetails.STATEFIPS}-${stateName}/${fipsDetails.PLACEFIPS}-${geoName}/${downloadRef}-${productRef}-${geoid}-${geoName}-${stateName}`;
  } else {
    throw new Error('unexpected sumlev.');
  }

  return key;
};

exports.removeS3Files = function (cleanupS3) {
  const s3 = new AWS.S3();

  const cleaned = cleanupS3.map(item => {
    if (item.type === s3deleteType.FILE) {
      return s3
        .deleteObject({
          Bucket: item.bucket,
          Key: item.key,
        })
        .promise();
    } else if (item.type === s3deleteType.DIRECTORY) {
      return emptyS3Directory(item.bucket, item.key);
    } else {
      throw new Error(`unexpected type: ${item.type} in removeS3Files`);
    }
  });

  return Promise.all(cleaned);
};
