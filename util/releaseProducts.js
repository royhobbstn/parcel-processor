const path = require('path');
const {
  directories,
  fileFormats,
  referenceIdLength,
  buckets,
  productOrigins,
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

  // upload ndgeojson and stat files to S3 (concurrent)
  await uploadProductFiles(productKey, outputPath);

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
  await putFileToS3(
    buckets.productsBucket,
    `${productKeyGeoJSON}.geojson`,
    `${outputPath}.geojson`,
    'application/geo+json',
    true,
  );

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
    await putFileToS3(
      buckets.productsBucket,
      `${productKeyGPKG}.gpkg`,
      `${outputPath}.gpkg`,
      'application/geopackage+sqlite3',
      true,
    );
  } catch (e) {
    console.error(e);
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
    await putFileToS3(
      buckets.productsBucket,
      `${productKeySHP}-shp.zip`,
      `${directories.outputDir}/${path.parse(productKeySHP).base}-shp.zip`,
      'application/zip',
      false,
    );
  } catch (e) {
    console.error(e);
  }

  return { productKey, productKeyGeoJSON, productKeyGPKG, productKeySHP };
};

exports.createTiles = async function (connection, meta) {
  const tilesDir = `${directories.processedDir}/${meta.downloadRef}-${meta.productRefTiles}`;
  const commandInput = await runTippecanoe(meta.outputPath, tilesDir);
  const maxZoom = getMaxDirectoryLevel(tilesDir);
  const metadata = { ...commandInput, maxZoom, ...meta, processed: new Date().toISOString() };
  // sync tiles
  await s3Sync(tilesDir, buckets.tilesBucket, `${meta.downloadRef}-${meta.productRefTiles}`);
  // write metadata
  await putTextToS3(
    buckets.tilesBucket,
    `${meta.downloadRef}-${meta.productRefTiles}/info.json`,
    JSON.stringify(metadata),
    'application/json',
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
};
