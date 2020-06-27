// @ts-check

const config = require('config');
const path = require('path');
const {
  directories,
  fileFormats,
  referenceIdLength,
  productOrigins,
  s3deleteType,
  modes,
} = require('./constants');
const { createProductDownloadKey } = require('./wrappers/wrapS3');
const { putFileToS3, putTextToS3, s3Sync } = require('./primitives/s3Operations');
const { queryCreateProductRecord } = require('./primitives/queries');
const {
  convertToFormat,
  spawnTippecane,
  writeTileAttributes,
  addClusterIdToGeoData,
  createNdGeoJsonWithClusterId,
} = require('./processGeoFile');
const { generateRef, gzipTileAttributes } = require('./crypto');
const { zipShapefile, getMaxDirectoryLevel } = require('./filesystemUtil');
const { log } = require('./logger');

exports.releaseProducts = async function (
  fipsDetails,
  geoid,
  geoName,
  downloadRef,
  downloadId,
  outputPath,
  cleanupS3,
  mode,
) {
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
  log.info(`created geoJSON product record.  ref: ${productRefGeoJSON}`);

  if (mode.label === modes.FULL_RUN.label) {
    await putFileToS3(
      config.get('Buckets.productsBucket'),
      `${productKeyGeoJSON}.geojson`,
      `${outputPath}.geojson`,
      'application/geo+json',
      true,
    );
    cleanupS3.push({
      bucket: config.get('Buckets.productsBucket'),
      key: `${productKeyGeoJSON}.geojson`,
      type: s3deleteType.FILE,
    });
    log.info(`uploaded geoJSON file to S3.  key: ${productKeyGeoJSON}`);
  }

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
      downloadId,
      productRefGPKG,
      fileFormats.GPKG.extension,
      productOrigins.ORIGINAL,
      geoid,
      `${productKeyGPKG}.gpkg`,
    );
    log.info(`created GPKG product record.  ref: ${productRefGPKG}`);

    if (mode.label === modes.FULL_RUN.label) {
      await putFileToS3(
        config.get('Buckets.productsBucket'),
        `${productKeyGPKG}.gpkg`,
        `${outputPath}.gpkg`,
        'application/geopackage+sqlite3',
        true,
      );
      cleanupS3.push({
        bucket: config.get('Buckets.productsBucket'),
        key: `${productKeyGPKG}.gpkg`,
        type: s3deleteType.FILE,
      });
      log.info(`uploaded GPKG file to S3.  key: ${productKeyGPKG}`);
    }
  } catch (e) {
    log.error(e);
    log.info(`ERROR:  !!! uploading or product record creation failed on GPKG.`);
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
      downloadId,
      productRefSHP,
      fileFormats.SHP.extension,
      productOrigins.ORIGINAL,
      geoid,
      `${productKeySHP}-shp.zip`,
    );
    log.info(`created SHP product record.  ref: ${productRefSHP}`);

    if (mode.label === modes.FULL_RUN.label) {
      await putFileToS3(
        config.get('Buckets.productsBucket'),
        `${productKeySHP}-shp.zip`,
        `${directories.outputDir}/${path.parse(productKeySHP).base}-shp.zip`,
        'application/zip',
        false,
      );
      cleanupS3.push({
        bucket: config.get('Buckets.productsBucket'),
        key: `${productKeySHP}-shp.zip`,
        type: s3deleteType.FILE,
      });
      log.info(`uploaded SHP file to S3  key: ${productKeySHP}`);
    }
  } catch (e) {
    log.error(e);
    log.info(`ERROR:  !!! uploading or product record creation failed on SHP.`);
  }
};

exports.createTiles = async function (meta, cleanupS3, points, propertyCount, mode) {
  // run kmeans geo cluster on data and create a lookup of idPrefix to clusterPrefix
  const lookup = addClusterIdToGeoData(points, propertyCount);

  // create derivative ndgeojson with clusterId
  const derivativePath = await createNdGeoJsonWithClusterId(meta.outputPath, lookup);

  const tilesDir = `${directories.processedDir}/${meta.downloadRef}-${meta.productRefTiles}`;

  // todo include cluster_id as property in tippecanoe
  const commandInput = await spawnTippecane(tilesDir, derivativePath);
  const maxZoom = getMaxDirectoryLevel(tilesDir);
  await writeTileAttributes(derivativePath, tilesDir);
  await gzipTileAttributes(`${tilesDir}/attributes`);
  const metadata = { ...commandInput, maxZoom, ...meta, processed: new Date().toISOString() };
  // sync tiles
  if (mode.label === modes.FULL_RUN.label) {
    await s3Sync(
      tilesDir,
      config.get('Buckets.tilesBucket'),
      `${meta.downloadRef}-${meta.productRefTiles}`,
    );
    log.info(`uploaded TILES directory to S3.  Dir: ${meta.downloadRef}-${meta.productRefTiles}`);
    cleanupS3.push({
      bucket: config.get('Buckets.tilesBucket'),
      key: `${meta.downloadRef}-${meta.productRefTiles}`,
      type: s3deleteType.DIRECTORY,
    });

    // write metadata
    await putTextToS3(
      config.get('Buckets.tilesBucket'),
      `${meta.downloadRef}-${meta.productRefTiles}/info.json`,
      JSON.stringify(metadata),
      'application/json',
      false,
    );

    log.info(
      `uploaded TILES meta file to S3.  Dir: ${meta.downloadRef}-${meta.productRefTiles}/info.json`,
    );
  }
  await queryCreateProductRecord(
    meta.downloadId,
    meta.productRefTiles,
    fileFormats.TILES.extension,
    productOrigins.ORIGINAL,
    meta.geoid,
    `${meta.downloadRef}-${meta.productRefTiles}`,
  );
  log.info(`created TILES product record.  ref: ${meta.productRefTiles}`);
};
