const path = require('path');
const { rawDir, rawBucket, productsBucket } = require('../constants');
const { putFileToS3 } = require('../primitives/s3Operations');
const { lookupState } = require('../lookupState');

exports.uploadRawFileToS3 = async function (filePath, downloadId) {
  // remove leading './' in rawDir path
  const rep = rawDir.replace('./', '');

  const originalFileName = filePath.split(`${rep}/`)[1];
  const s3KeyName = `${downloadId}-${originalFileName}`;

  const extension = path.extname(originalFileName);

  if (extension !== '.zip') {
    throw new Error('expected filename extension to be .zip');
  }

  // save downloaded file to the raw bucket in s3 (don't gzip here)
  await putFileToS3(rawBucket, s3KeyName, filePath, 'application/zip', false);
};

exports.uploadProductFiles = async function (
  downloadId,
  productId,
  geoName,
  geoid,
  fipsDetails,
  outputPath,
) {
  const stateName = lookupState(fipsDetails.STATEFIPS).replace(/[^a-z0-9]+/gi, '-');

  let key;

  if (fipsDetails.SUMLEV === '040' || fipsDetails.SUMLEV === '050') {
    key = `${fipsDetails.STATEFIPS}-${stateName}/${fipsDetails.COUNTYFIPS}-${geoName}/${downloadId}-${productId}-${geoid}-${geoName}-${stateName}`;
  } else if (fipsDetails.SUMLEV === '160') {
    key = `${fipsDetails.STATEFIPS}-${stateName}/${fipsDetails.PLACEFIPS}-${geoName}/${downloadId}-${productId}-${geoid}-${geoName}-${stateName}`;
  } else {
    throw new Error('unexpected sumlev.');
  }

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
    'application/x-ndjson',
    true,
  );

  try {
    await Promise.all([statFile, ndgeojsonFile]);
    console.log('Output files were successfully loaded to S3');
  } catch (err) {
    console.error('Error uploading output files to S3');
    console.error(err);
    process.exit();
  }

  return;
};
