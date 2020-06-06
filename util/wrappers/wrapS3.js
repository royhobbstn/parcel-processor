const path = require('path');
const { rawBucket, productsBucket } = require('../constants');
const { putFileToS3 } = require('../primitives/s3Operations');
const { lookupState } = require('../lookupState');

exports.uploadRawFileToS3 = async function (filePath, rawKey) {
  const extension = path.extname(rawKey);

  if (extension !== '.zip') {
    throw new Error('expected filename extension to be .zip');
  }

  // save downloaded file to the raw bucket in s3 (don't gzip here)
  await putFileToS3(rawBucket, rawKey, filePath, 'application/zip', false);
};

exports.uploadProductFiles = async function (key, outputPath) {
  const statFile = putFileToS3(
    productsBucket,
    `${key}-stat.json`,
    `${outputPath}.json`,
    'application/json',
    true,
  );
  const ndgeojsonFile = putFileToS3(
    productsBucket,
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
