const path = require('path');
const {
  directories,
  fileFormats,
  referenceIdLength,
  buckets,
  productOrigins,
  s3deleteType,
} = require('./constants');
const { uploadProductFiles, createProductDownloadKey } = require('./wrappers/wrapS3');
const { putFileToS3, putTextToS3, s3Sync } = require('./primitives/s3Operations');
const { queryCreateProductRecord } = require('./primitives/queries');
const { convertToFormat, runTippecanoe } = require('./processGeoFile');
const { generateRef } = require('./crypto');
const { zipShapefile, getMaxDirectoryLevel } = require('./filesystemUtil');

exports.releaseProducts = async function (
  connection,
  fipsDetails,
  geoid,
  geoName,
  downloadRef,
  downloadId,
  outputPath,
  executiveSummary,
  cleanupS3,
) {
  const productRef = generateRef(referenceIdLength);

  // contains no file extension.  Used as base key for -stat.json  and .ndgeojson
  const productKey = createProductDownloadKey(fipsDetails, geoid, geoName, downloadRef, productRef);

  await queryCreateProductRecord(
    connection,
    downloadId,
    productRef,
    fileFormats.NDGEOJSON.extension,
    productOrigins.ORIGINAL,
    geoid,
    `${productKey}.ndgeojson`,
  );
  executiveSummary.push(`wrote NDgeoJSON product record.  ref: ${productRef}`);

  // upload ndgeojson and stat files to S3 (concurrent)
  await uploadProductFiles(productKey, outputPath);
  cleanupS3.push({
    bucket: buckets.productsBucket,
    key: `${productKey}-stat.json`,
    type: s3deleteType.FILE,
  });
  cleanupS3.push({
    bucket: buckets.productsBucket,
    key: `${productKey}.ndgeojson`,
    type: s3deleteType.FILE,
  });
  executiveSummary.push(`uploaded NDgeoJSON and '-stat.json' files to S3.  key: ${productKey}`);

  // ADDITIONAL FORMATS //

  await convertToFormat(fileFormats.GEOJSON, outputPath);
  const productRefGeoJSON = generateRef(referenceIdLength);
  const productKeyGeoJSON = createProductDownloadKey(
    fipsDetails,
    geoid,
    geoName,
    downloadRef,
    productRefGeoJSON,
  );

  await queryCreateProductRecord(
    connection,
    downloadId,
    productRefGeoJSON,
    fileFormats.GEOJSON.extension,
    productOrigins.ORIGINAL,
    geoid,
    `${productKeyGeoJSON}.geojson`,
  );
  executiveSummary.push(`created geoJSON product record.  ref: ${productRefGeoJSON}`);

  await putFileToS3(
    buckets.productsBucket,
    `${productKeyGeoJSON}.geojson`,
    `${outputPath}.geojson`,
    'application/geo+json',
    true,
  );
  cleanupS3.push({
    bucket: buckets.productsBucket,
    key: `${productKeyGeoJSON}.geojson`,
    type: s3deleteType.FILE,
  });
  executiveSummary.push(`uploaded geoJSON file to S3.  key: ${productKeyGeoJSON}`);

  // file conversion is bound to be problematic for GPKG and SHP
  // dont crash program on account of failures here

  let productKeyGPKG;
  let productKeySHP;

  try {
    await convertToFormat(fileFormats.GPKG, outputPath);
    const productRefGPKG = generateRef(referenceIdLength);
    productKeyGPKG = createProductDownloadKey(
      fipsDetails,
      geoid,
      geoName,
      downloadRef,
      productRefGPKG,
    );
    await queryCreateProductRecord(
      connection,
      downloadId,
      productRefGPKG,
      fileFormats.GPKG.extension,
      productOrigins.ORIGINAL,
      geoid,
      `${productKeyGPKG}.gpkg`,
    );
    executiveSummary.push(`created GPKG product record.  ref: ${productRefGPKG}`);

    await putFileToS3(
      buckets.productsBucket,
      `${productKeyGPKG}.gpkg`,
      `${outputPath}.gpkg`,
      'application/geopackage+sqlite3',
      true,
    );
    cleanupS3.push({
      bucket: buckets.productsBucket,
      key: `${productKeyGPKG}.gpkg`,
      type: s3deleteType.FILE,
    });
    executiveSummary.push(`uploaded GPKG file to S3.  key: ${productKeyGPKG}`);
  } catch (e) {
    console.error(e);
    executiveSummary.push(`ERROR:  !!! uploading or product record creation failed on GPKG.`);
  }

  try {
    await convertToFormat(fileFormats.SHP, outputPath);
    const productRefSHP = generateRef(referenceIdLength);
    productKeySHP = createProductDownloadKey(
      fipsDetails,
      geoid,
      geoName,
      downloadRef,
      productRefSHP,
    );
    await zipShapefile(outputPath, productKeySHP);
    await queryCreateProductRecord(
      connection,
      downloadId,
      productRefSHP,
      fileFormats.SHP.extension,
      productOrigins.ORIGINAL,
      geoid,
      `${productKeySHP}-shp.zip`,
    );
    executiveSummary.push(`created SHP product record.  ref: ${productRefSHP}`);

    await putFileToS3(
      buckets.productsBucket,
      `${productKeySHP}-shp.zip`,
      `${directories.outputDir}/${path.parse(productKeySHP).base}-shp.zip`,
      'application/zip',
      false,
    );
    cleanupS3.push({
      bucket: buckets.productsBucket,
      key: `${productKeySHP}-shp.zip`,
      type: s3deleteType.FILE,
    });
    executiveSummary.push(`uploaded SHP file to S3  key: ${productKeySHP}`);
  } catch (e) {
    console.error(e);
    executiveSummary.push(`ERROR:  !!! uploading or product record creation failed on SHP.`);
  }

  return { productKey, productKeyGeoJSON, productKeyGPKG, productKeySHP };
};

exports.createTiles = async function (connection, meta, executiveSummary, cleanupS3) {
  const tilesDir = `${directories.processedDir}/${meta.downloadRef}-${meta.productRefTiles}`;
  const commandInput = await runTippecanoe(meta.outputPath, tilesDir);
  await writeTileAttributes(meta.outputPath, tilesDir);
  const maxZoom = getMaxDirectoryLevel(tilesDir);
  const metadata = { ...commandInput, maxZoom, ...meta, processed: new Date().toISOString() };
  // sync tiles
  await s3Sync(tilesDir, buckets.tilesBucket, `${meta.downloadRef}-${meta.productRefTiles}`);
  executiveSummary.push(
    `uploaded TILES directory to S3.  Dir: ${meta.downloadRef}-${meta.productRefTiles}`,
  );
  cleanupS3.push({
    bucket: buckets.tilesBucket,
    key: `${meta.downloadRef}-${meta.productRefTiles}`,
    type: s3deleteType.DIRECTORY,
  });

  // write metadata
  await putTextToS3(
    buckets.tilesBucket,
    `${meta.downloadRef}-${meta.productRefTiles}/info.json`,
    JSON.stringify(metadata),
    'application/json',
  );

  executiveSummary.push(
    `uploaded TILES meta file to S3.  Dir: ${meta.downloadRef}-${meta.productRefTiles}/info.json`,
  );

  await queryCreateProductRecord(
    connection,
    meta.downloadId,
    meta.productRefTiles,
    fileFormats.TILES.extension,
    productOrigins.ORIGINAL,
    meta.geoid,
    `${meta.downloadRef}-${meta.productRefTiles}`,
  );
  executiveSummary.push(`created TILES product record.  ref: ${meta.productRefTiles}`);
};
