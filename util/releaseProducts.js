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
