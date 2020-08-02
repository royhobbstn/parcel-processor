// @ts-check

const AWS = require('aws-sdk');
const path = require('path');
const { s3deleteType } = require('./constants');
const { putFileToS3, emptyS3Directory } = require('./s3Operations');
const { lookupState } = require('./lookupState');
const config = require('config');
const { unwindStack } = require('./misc');

exports.S3Writes = async function (ctx, cleanupS3, filePath, rawKey, productKey, outputPath) {
  ctx.process.push('S3Writes');

  await uploadRawFileToS3(ctx, filePath, rawKey);
  cleanupS3.push({
    bucket: config.get('Buckets.rawBucket'),
    key: rawKey,
    type: s3deleteType.FILE,
  });
  ctx.log.info(`uploaded raw file to S3.  key: ${rawKey}`);
  await uploadProductFiles(ctx, productKey, outputPath);
  cleanupS3.push({
    bucket: config.get('Buckets.productsBucket'),
    key: `${productKey}-stat.json`,
    type: s3deleteType.FILE,
  });
  cleanupS3.push({
    bucket: config.get('Buckets.productsBucket'),
    key: `${productKey}.ndgeojson`,
    type: s3deleteType.FILE,
  });
  ctx.log.info(`uploaded NDgeoJSON and '-stat.json' files to S3.  key: ${productKey}`);
  unwindStack(ctx.process, 'S3Writes');
};

exports.uploadRawFileToS3 = uploadRawFileToS3;

async function uploadRawFileToS3(ctx, filePath, rawKey) {
  ctx.process.push('uploadRawFileToS3');

  const extension = path.extname(rawKey);

  if (extension !== '.zip') {
    throw new Error('expected filename extension to be .zip');
  }

  // save downloaded file to the raw bucket in s3 (don't gzip here)
  await putFileToS3(
    ctx,
    config.get('Buckets.rawBucket'),
    rawKey,
    filePath,
    'application/zip',
    false,
    `attachment; filename="${path.basename(rawKey)}"`,
  );
  unwindStack(ctx.process, 'uploadRawFileToS3');
}

exports.uploadProductFiles = uploadProductFiles;

async function uploadProductFiles(ctx, key, outputPath) {
  ctx.process.push('uploadProductFiles');

  const statFile = putFileToS3(
    ctx,
    config.get('Buckets.productsBucket'),
    `${key}-stat.json`,
    `${outputPath}.json`,
    'application/json',
    true,
  );
  const ndgeojsonFile = putFileToS3(
    ctx,
    config.get('Buckets.productsBucket'),
    `${key}.ndgeojson`,
    `${outputPath}.ndgeojson`,
    'application/geo+json-seq',
    true,
  );

  await Promise.all([statFile, ndgeojsonFile]);
  ctx.log.info('Output files were successfully loaded to S3');

  unwindStack(ctx.process, 'uploadProductFiles');
}

exports.createRawDownloadKey = function (ctx, fipsDetails, geoid, geoName, downloadRef) {
  ctx.process.push('createRawDownloadKey');

  const stateName = lookupState(ctx, fipsDetails.STATEFIPS).replace(/[^a-z0-9]+/gi, '-');

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

  unwindStack(ctx.process, 'createRawDownloadKey');
  return `${key}.zip`;
};

exports.createProductDownloadKey = function (
  ctx,
  fipsDetails,
  geoid,
  geoName,
  downloadRef,
  productRef,
  individualRef,
) {
  ctx.process.push('createProductDownloadKey');

  const stateName = lookupState(ctx, fipsDetails.STATEFIPS).replace(/[^a-z0-9]+/gi, '-');

  let key;

  if (fipsDetails.SUMLEV === '040') {
    key = `${fipsDetails.STATEFIPS}-${stateName}/${fipsDetails.COUNTYFIPS}-${geoName}/${downloadRef}-${productRef}-${individualRef}-${geoid}-${geoName}`;
  } else if (fipsDetails.SUMLEV === '050') {
    key = `${fipsDetails.STATEFIPS}-${stateName}/${fipsDetails.COUNTYFIPS}-${geoName}/${downloadRef}-${productRef}-${individualRef}-${geoid}-${geoName}-${stateName}`;
  } else if (fipsDetails.SUMLEV === '160') {
    key = `${fipsDetails.STATEFIPS}-${stateName}/${fipsDetails.PLACEFIPS}-${geoName}/${downloadRef}-${productRef}-${individualRef}-${geoid}-${geoName}-${stateName}`;
  } else {
    throw new Error('unexpected sumlev.');
  }

  unwindStack(ctx.process, 'createProductDownloadKey');

  return key;
};

exports.removeS3Files = function (ctx, cleanupS3) {
  ctx.process.push('removeS3Files');

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
      return emptyS3Directory(ctx, item.bucket, item.key);
    } else {
      throw new Error(`unexpected type: ${item.type} in removeS3Files`);
    }
  });

  unwindStack(ctx.process, 'removeS3Files');

  return Promise.all(cleaned);
};
