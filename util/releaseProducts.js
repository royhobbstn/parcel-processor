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
  try {
    await convertToFormat(fileFormats.GPKG, outputPath);
    const productRefGPKG = generateRef(referenceIdLength);
    const productKeyGPKG = createProductDownloadKey(
      fipsDetails,
      geoid,
      geoName,
      downloadRef,
      productRefGPKG,
    );
    await queryCreateProductRecord(
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
    const productKeySHP = createProductDownloadKey(
      fipsDetails,
      geoid,
      geoName,
      downloadRef,
      productRefSHP,
    );
    await zipShapefile(outputPath, productKeySHP);
    await queryCreateProductRecord(
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
};

exports.createTiles = async function (outputPath, downloadRef, productRefTiles) {
  const tilesDir = `${directories.processedDir}/${downloadRef}-${productRefTiles}`;
  const commandInput = await runTippecanoe(outputPath, tilesDir);
  const maxZoom = getMaxDirectoryLevel(tilesDir);
  // todo so much more... original filename, webpage, date processed, everything at all
  // todo like links to download in SHP, GPKG, GeoJSON, Original
  const metadata = { ...commandInput, maxZoom };
  // sync tiles
  await s3Sync(tilesDir, buckets.tilesBucket, `${downloadRef}-${productRefTiles}`);
  // write metadata
  await putTextToS3(
    buckets.tilesBucket,
    `${downloadRef}-${productRefTiles}/info.json`,
    JSON.stringify(metadata),
    'application/json',
  );
};
