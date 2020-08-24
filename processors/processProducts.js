// @ts-check

const config = require('config');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');
const {
  directories,
  fileFormats,
  referenceIdLength,
  productOrigins,
  s3deleteType,
  messageTypes,
  zoomLevels,
} = require('../util/constants');
const { acquireConnection } = require('../util/wrapQuery');
const { createProductDownloadKey, removeS3Files } = require('../util/wrapS3');
const { putFileToS3, streamS3toFileSystem, putTextToS3, s3Sync } = require('../util/s3Operations');
const {
  queryCreateProductRecord,
  checkForProducts,
  createMessageRecord,
} = require('../util/queries');
const {
  convertToFormat,
  writeTileAttributes,
  addClusterIdToGeoData,
  createClusterIdHull,
  extractPointsFromNdGeoJson,
  readTippecanoeMetadata,
  writeMbTiles,
  tileJoinLayers,
} = require('../util/processGeoFile');
const { generateRef, gzipTileAttributes } = require('../util/crypto');
const { zipShapefile, createDirectories } = require('../util/filesystemUtil');
const { unwindStack } = require('../util/misc');
const { runAggregate } = require('../aggregate/aggregate');
const { clusterAggregated } = require('../aggregate/clusterAggregated');

exports.processProducts = async function (ctx, data) {
  ctx.process.push('processProducts');

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
  //   productKey: '15-Hawaii/007-Kauai-County/5fe3a581-6f612f99-15007-Kauai-County-Hawaii',
  // };

  ctx.messageId = data.Messages[0].MessageId;
  ctx.type = messageTypes.PRODUCT;
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
  let caughtError = false;

  const fileNameBase = productKey.split('/').slice(-1)[0];
  const destPlain = `${directories.productTempDir + ctx.directoryId}/${fileNameBase}.ndgeojson.gz`;
  const destUnzipped = `${directories.productTempDir + ctx.directoryId}/${fileNameBase}.ndgeojson`;
  const convertToFormatBase = `${directories.productTempDir + ctx.directoryId}/${fileNameBase}`;

  ctx.log.info('paths', {
    fileNameBase,
    destPlain,
    destUnzipped,
    convertToFormatBase,
  });

  // before processing, do a check to make sure there is not already a product with same geoid / downloadRef, productRef combination.
  // SQS can sometimes duplicate messages, and this would guard against it.
  ctx.log.info('Checking for existing products....');
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

  ctx.log.info('existing products', { existingProducts });

  try {
    ctx.log.info('Beginning file download from S3.');
    await streamS3toFileSystem(
      ctx,
      config.get('Buckets.productsBucket'),
      `${productKey}.ndgeojson`,
      destPlain,
      destUnzipped,
    );
  } catch (err) {
    caughtError = true;
    ctx.log.info(`Error streaming the file from S3.`, {
      data: err.message,
      stack: err.stack,
    });
  }

  // GEOJSON
  if (existingGeoJson) {
    ctx.log.info('GeoJson product for this Geoid and DownloadId already exists.  Skipping.');
  }
  if (messagePayload.products.includes(fileFormats.GEOJSON.label) && !existingGeoJson) {
    ctx.log.info('Starting GeoJson Product Creation');
    const cleanupS3 = [];

    try {
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
          `attachment; filename="${path.basename(productKeyGeoJSON)}.geojson"`,
        );
        cleanupS3.push({
          bucket: config.get('Buckets.productsBucket'),
          key: `${productKeyGeoJSON}.geojson`,
          type: s3deleteType.FILE,
        });
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
          ctx.messageId,
        );
        ctx.log.info(`created geoJSON product record.  ref: ${individualRefGeoJson}`);
      }
    } catch (err) {
      // unlike in processSort, we won't let a failure in a product creation
      // cancel processing of the other products
      // but we will keep track of it for email notification purposes.
      await removeS3Files(ctx, cleanupS3);
      caughtError = true;
      ctx.log.info(`Uploading or product record creation failed on GeoJSON`, {
        data: err.message,
        stack: err.stack,
      });
    }
  }

  // file conversion is bound to be problematic for GPKG and SHP
  // dont crash program on account of failures here

  // GPKG
  if (existingGpkg) {
    ctx.log.info('GeoPackage product for this Geoid and DownloadId already exists.  Skipping.');
  }
  if (messagePayload.products.includes(fileFormats.GPKG.label) && !existingGpkg) {
    ctx.log.info('Starting GPKG Product Creation');

    const cleanupS3 = [];

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
          `attachment; filename="${path.basename(productKeyGPKG)}.gpkg"`,
        );
        cleanupS3.push({
          bucket: config.get('Buckets.productsBucket'),
          key: `${productKeyGPKG}.gpkg`,
          type: s3deleteType.FILE,
        });
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
          ctx.messageId,
        );
        ctx.log.info(`created GPKG product record.  ref: ${individualRefGPKG}`);
      }
    } catch (err) {
      await removeS3Files(ctx, cleanupS3);
      caughtError = true;
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
    ctx.log.info('Starting Shapefile Product Creation');

    const cleanupS3 = [];

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
          `attachment; filename="${path.basename(productKeySHP)}-shp.zip"`,
        );
        cleanupS3.push({
          bucket: config.get('Buckets.productsBucket'),
          key: `${productKeySHP}-shp.zip`,
          type: s3deleteType.FILE,
        });
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
          ctx.messageId,
        );
        ctx.log.info(`created SHP product record.  ref: ${individualRefSHP}`);
      }
    } catch (err) {
      await removeS3Files(ctx, cleanupS3);
      caughtError = true;
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
    ctx.log.info('Starting Tiles Product Creation');

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

    const cleanupS3 = [];

    try {
      const [points, propertyCount] = await extractPointsFromNdGeoJson(ctx, convertToFormatBase);

      // run kmeans geo cluster on data and create a lookup of idPrefix to clusterPrefix
      const lookup = addClusterIdToGeoData(ctx, points, propertyCount);
      const clusterHull = await createClusterIdHull(ctx, convertToFormatBase, lookup);

      const dirName = `${downloadRef}-${productRef}-${productRefTiles}`;
      const tilesDir = `${directories.tilesDir + ctx.directoryId}/${dirName}`;

      const featureProperties = await runAggregate(ctx, convertToFormatBase);

      await clusterAggregated(ctx, tilesDir, featureProperties, convertToFormatBase);

      for (
        let currentZoom = zoomLevels.LOW;
        currentZoom <= zoomLevels.HIGH;
        currentZoom = currentZoom + 2
      ) {
        const findOriginalFile = currentZoom === zoomLevels.HIGH;
        let filename = '';

        if (!findOriginalFile) {
          // get from aggregated geojson outputs
          filename = `${
            directories.productTempDir + ctx.directoryId
          }/aggregated_${currentZoom}.json`;
        } else {
          // get from original ndgeojson
          filename = `${convertToFormatBase}.ndgeojson`;
        }

        await writeMbTiles(ctx, filename, currentZoom);
      }

      const commandInput = await tileJoinLayers(ctx, tilesDir);

      ctx.log.info(`writing cluster hull`);
      const buffer = zlib.gzipSync(JSON.stringify(clusterHull));
      fs.writeFileSync(`${tilesDir}/cluster_hull.geojson`, buffer);

      await writeTileAttributes(ctx, convertToFormatBase, tilesDir, lookup);
      await gzipTileAttributes(ctx, `${tilesDir}/attributes`);

      const generatedMetadata = await readTippecanoeMetadata(ctx, `${tilesDir}/metadata.json`);

      const metadata = {
        layername: '',
        ...commandInput,
        ...meta,
        processed: new Date().toISOString(),
        generatedMetadata,
      };

      // sync tiles
      if (!isDryRun) {
        ctx.log.info('syncing TILES to S3');
        await s3Sync(ctx, tilesDir, config.get('Buckets.tilesBucket'), dirName);
        ctx.log.info(`finished!  uploaded TILES directory to S3.  Dir: ${dirName}`);
        cleanupS3.push({
          bucket: config.get('Buckets.tilesBucket'),
          key: tilesDir,
          type: s3deleteType.DIRECTORY,
        });

        // TODO cant you just put this into the local dir and have s3 sync take care of it?
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
          ctx.messageId,
        );
        ctx.log.info(`created TILES product record.  ref: ${productRefTiles}`);
      }
    } catch (err) {
      await removeS3Files(ctx, cleanupS3);
      caughtError = true;
      ctx.log.error(`Uploading or product record creation failed on Tiles`, {
        error: err.message,
        stack: err.stack,
      });
    }
  }

  await createMessageRecord(ctx, ctx.messageId, JSON.stringify(messagePayload), ctx.type);
  ctx.log.info('Message reference record was created.');

  ctx.log.info('product creation finished');

  if (caughtError) {
    throw new Error('There were issues with one or more products.  See logs.');
  }

  unwindStack(ctx.process, 'processProducts');
};
