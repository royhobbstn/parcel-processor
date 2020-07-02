// @ts-check

const config = require('config');
const path = require('path');
const {
  directories,
  fileFormats,
  referenceIdLength,
  productOrigins,
} = require('../util/constants');
const { acquireConnection } = require('../util/wrapQuery');
const { createProductDownloadKey } = require('../util/wrapS3');
const { putFileToS3, streamS3toFileSystem, putTextToS3, s3Sync } = require('../util/s3Operations');
const { queryCreateProductRecord, checkForProducts } = require('../util/queries');
const {
  convertToFormat,
  spawnTippecane,
  writeTileAttributes,
  addClusterIdToGeoData,
  createNdGeoJsonWithClusterId,
  extractPointsFromNdGeoJson,
} = require('../util/processGeoFile');
const { generateRef, gzipTileAttributes } = require('../util/crypto');
const { zipShapefile, getMaxDirectoryLevel, createDirectories } = require('../util/filesystemUtil');

exports.processProducts = async function (ctx, data) {
  await acquireConnection(ctx);

  await createDirectories(ctx, [
    directories.outputDir,
    directories.productTempDir,
    directories.logDir,
    directories.tilesDir,
  ]);

  // const messagePayload = {
  //   dryRun: false,
  //   products: [
  //     fileFormats.GEOJSON.label,
  //     fileFormats.GPKG.label,
  //     fileFormats.SHP.label,
  //     fileFormats.TILES.label,
  //   ],
  //   productRef: '6f612f99',
  //   productOrigin: productOrigins.DERIVED,
  //   fipsDetails: {
  //     SUMLEV: '050',
  //     STATEFIPS: '15',
  //     COUNTYFIPS: '007',
  //     PLACEFIPS: '',
  //   },
  //   geoid: '15007',
  //   geoName: 'Kauai-County',
  //   downloadRef: '5fe3a581',
  //   downloadId: 3,
  //   productKey: '15-Hawaii/007-Kauai-County/5fe3a581-6f612f99-15007-Kauai-County-Hawaii.ndgeojson',
  // };

  const messagePayload = JSON.parse(data.Messages[0].Body);
  ctx.log.info('Processing Message', { messagePayload });
  ctx.isDryRun = messagePayload.dryRun;

  const isDryRun = messagePayload.dryRun;
  const productRef = messagePayload.productRef;
  const productKey = messagePayload.productKey;
  const downloadId = messagePayload.downloadId;
  const geoid = messagePayload.geoid;
  const geoName = messagePayload.geoName;
  const downloadRef = messagePayload.downloadRef;
  const productOrigin = messagePayload.productOrigin;
  const fipsDetails = messagePayload.fipsDetails;

  const fileNameBase = productKey.split('/').slice(-1)[0];
  const fileNameNoExtension = fileNameBase.split('.').slice(0, -1)[0];
  const destPlain = `${directories.productTempDir + ctx.directoryId}/${fileNameBase}.gz`;
  const destUnzipped = `${directories.productTempDir + ctx.directoryId}/${fileNameBase}`;
  const convertToFormatBase = `${
    directories.productTempDir + ctx.directoryId
  }/${fileNameNoExtension}`;

  ctx.log.info({ fileNameBase, fileNameNoExtension, destPlain, destUnzipped, convertToFormatBase });

  // before processing, do a check to make sure there is not already a product with same geoid / downloadRef, productRef combination.
  // SQS can sometimes duplicate messages, and this would guard against it.
  const existingProducts = await checkForProducts(ctx, geoid, downloadId);
  const existingGeoJson = existingProducts.some(product => {
    return product.product_type === fileFormats.GEOJSON.extension;
  });
  const existingGpkg = existingProducts.some(product => {
    return product.product_type === fileFormats.GPKG.extension;
  });
  const existingShp = existingProducts.some(product => {
    return product.product_type === fileFormats.SHP.extension;
  });
  const existingPbf = existingProducts.some(product => {
    return product.product_type === fileFormats.TILES.extension;
  });

  await streamS3toFileSystem(
    ctx,
    config.get('Buckets.productsBucket'),
    productKey,
    destPlain,
    destUnzipped,
  );

  // GEOJSON
  if (existingGeoJson) {
    ctx.log.info('GeoJson product for this Geoid and DownloadId already exists.  Skipping.');
  }
  if (messagePayload.products.includes(fileFormats.GEOJSON.label) && !existingGeoJson) {
    await convertToFormat(ctx, fileFormats.GEOJSON, convertToFormatBase);

    const individualRefGeoJson = generateRef(ctx, referenceIdLength);

    const productKeyGeoJSON = createProductDownloadKey(
      ctx,
      fipsDetails,
      geoid,
      geoName,
      downloadRef,
      productRef,
      individualRefGeoJson,
    );

    if (!isDryRun) {
      await putFileToS3(
        ctx,
        config.get('Buckets.productsBucket'),
        `${productKeyGeoJSON}.geojson`,
        `${convertToFormatBase}.geojson`,
        'application/geo+json',
        true,
      );
      ctx.log.info(`uploaded geoJSON file to S3.  key: ${productKeyGeoJSON}`);

      await queryCreateProductRecord(
        ctx,
        downloadId,
        productRef,
        individualRefGeoJson,
        fileFormats.GEOJSON.extension,
        productOrigin,
        geoid,
        `${productKeyGeoJSON}.geojson`,
      );
      ctx.log.info(`created geoJSON product record.  ref: ${individualRefGeoJson}`);
    }
  }

  // file conversion is bound to be problematic for GPKG and SHP
  // dont crash program on account of failures here

  // GPKG
  if (existingGpkg) {
    ctx.log.info('GeoPackage product for this Geoid and DownloadId already exists.  Skipping.');
  }
  if (messagePayload.products.includes(fileFormats.GPKG.label) && !existingGpkg) {
    try {
      await convertToFormat(ctx, fileFormats.GPKG, convertToFormatBase);
      const individualRefGPKG = generateRef(ctx, referenceIdLength);
      const productKeyGPKG = createProductDownloadKey(
        ctx,
        fipsDetails,
        geoid,
        geoName,
        downloadRef,
        productRef,
        individualRefGPKG,
      );

      if (!isDryRun) {
        await putFileToS3(
          ctx,
          config.get('Buckets.productsBucket'),
          `${productKeyGPKG}.gpkg`,
          `${convertToFormatBase}.gpkg`,
          'application/geopackage+sqlite3',
          true,
        );
        ctx.log.info(`uploaded GPKG file to S3.  key: ${productKeyGPKG}`);

        await queryCreateProductRecord(
          ctx,
          downloadId,
          productRef,
          individualRefGPKG,
          fileFormats.GPKG.extension,
          productOrigin,
          geoid,
          `${productKeyGPKG}.gpkg`,
        );
        ctx.log.info(`created GPKG product record.  ref: ${individualRefGPKG}`);
      }
    } catch (err) {
      ctx.log.error(`Uploading or product record creation failed on GPKG`, {
        err: err.message,
        stack: err.stack,
      });
    }
  }

  // SHP
  if (existingShp) {
    ctx.log.info('Shapefile product for this Geoid and DownloadId already exists.  Skipping.');
  }
  if (messagePayload.products.includes(fileFormats.SHP.label) && !existingShp) {
    try {
      await convertToFormat(ctx, fileFormats.SHP, convertToFormatBase);
      const individualRefSHP = generateRef(ctx, referenceIdLength);
      const productKeySHP = createProductDownloadKey(
        ctx,
        fipsDetails,
        geoid,
        geoName,
        downloadRef,
        productRef,
        individualRefSHP,
      );
      await zipShapefile(ctx, convertToFormatBase, productKeySHP);

      if (!isDryRun) {
        await putFileToS3(
          ctx,
          config.get('Buckets.productsBucket'),
          `${productKeySHP}-shp.zip`,
          `${directories.outputDir + ctx.directoryId}/${path.parse(productKeySHP).base}-shp.zip`,
          'application/zip',
          false,
        );
        ctx.log.info(`uploaded SHP file to S3  key: ${productKeySHP}`);

        await queryCreateProductRecord(
          ctx,
          downloadId,
          productRef,
          individualRefSHP,
          fileFormats.SHP.extension,
          productOrigin,
          geoid,
          `${productKeySHP}-shp.zip`,
        );
        ctx.log.info(`created SHP product record.  ref: ${individualRefSHP}`);
      }
    } catch (err) {
      ctx.log.info(`Uploading or product record creation failed on SHP`, {
        data: err.message,
        stack: err.stack,
      });
    }
  }

  // TILES
  // but not on State Data
  if (existingPbf) {
    ctx.log.info('Tiles product for this Geoid and DownloadId already exists.  Skipping.');
  }
  if (
    messagePayload.products.includes(fileFormats.TILES.label) &&
    fipsDetails.SUMLEV !== '040' &&
    !existingPbf
  ) {
    const productRefTiles = generateRef(ctx, referenceIdLength);

    const meta = {
      geoid,
      geoName,
      fipsDetails,
      downloadId,
      downloadRef,
      productRef,
      productRefTiles,
    };

    const [points, propertyCount] = await extractPointsFromNdGeoJson(ctx, convertToFormatBase);

    // run kmeans geo cluster on data and create a lookup of idPrefix to clusterPrefix
    const lookup = addClusterIdToGeoData(ctx, points, propertyCount);

    // create derivative ndgeojson with clusterId
    const derivativePath = await createNdGeoJsonWithClusterId(ctx, convertToFormatBase, lookup);

    const dirName = `${downloadRef}-${productRef}-${productRefTiles}`;
    const tilesDir = `${directories.tilesDir + ctx.directoryId}/${dirName}`;

    // todo include cluster_id as property in tippecanoe
    const commandInput = await spawnTippecane(ctx, tilesDir, derivativePath);
    const maxZoom = getMaxDirectoryLevel(ctx, tilesDir);
    await writeTileAttributes(ctx, derivativePath, tilesDir);
    await gzipTileAttributes(ctx, `${tilesDir}/attributes`);
    const metadata = { ...commandInput, maxZoom, ...meta, processed: new Date().toISOString() };

    // sync tiles
    if (!isDryRun) {
      await s3Sync(ctx, tilesDir, config.get('Buckets.tilesBucket'), dirName);
      ctx.log.info(`uploaded TILES directory to S3.  Dir: ${dirName}`);

      // write metadata
      await putTextToS3(
        ctx,
        config.get('Buckets.tilesBucket'),
        `${dirName}/info.json`,
        JSON.stringify(metadata),
        'application/json',
        false,
      );
      ctx.log.info(`uploaded TILES meta file to S3.  Dir: ${dirName}/info.json`);

      await queryCreateProductRecord(
        ctx,
        downloadId,
        productRef,
        productRefTiles,
        fileFormats.TILES.extension,
        productOrigins.ORIGINAL,
        geoid,
        dirName,
      );
      ctx.log.info(`created TILES product record.  ref: ${productRefTiles}`);
    }
  }

  // todo cleanup

  ctx.log.info('product creation finished');
};
