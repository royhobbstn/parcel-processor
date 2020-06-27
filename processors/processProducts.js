// @ts-check

const config = require('config');
const path = require('path');
const {
  directories,
  fileFormats,
  referenceIdLength,
  productOrigins,
} = require('../util/constants');
const { acquireConnection } = require('../util/wrappers/wrapQuery');
const { createProductDownloadKey } = require('../util/wrappers/wrapS3');
const { putFileToS3, streamS3toFileSystem } = require('../util/primitives/s3Operations');
const { queryCreateProductRecord } = require('../util/primitives/queries');
const { convertToFormat } = require('../util/processGeoFile');
const { generateRef } = require('../util/crypto');
const { doBasicCleanup } = require('../util/cleanup');
const { zipShapefile } = require('../util/filesystemUtil');
const { log } = require('../util/logger');

exports.processProducts = async function () {
  await acquireConnection();

  // to avoid uploading anything from a previous run
  await doBasicCleanup([directories.productTemp], true, true);

  const messagePayload = {
    dryRun: true,
    products: [
      fileFormats.GEOJSON.label,
      fileFormats.GPKG.label,
      fileFormats.SHP.label,
      fileFormats.TILES.label,
    ],
    productRef: '6f612f99',
    productOrigin: productOrigins.DERIVED,
    fipsDetails: {
      SUMLEV: '050',
      STATEFIPS: '15',
      COUNTYFIPS: '007',
      PLACEFIPS: '',
    },
    geoid: '15007',
    geoName: 'Kauai-County',
    downloadRef: '5fe3a581',
    downloadId: 3,
    productKey: '15-Hawaii/007-Kauai-County/5fe3a581-6f612f99-15007-Kauai-County-Hawaii.ndgeojson',
  };

  // const messagePayload = JSON.parse(data.Messages[0].Body);
  console.log(messagePayload);

  const isDryRun = messagePayload.dryRun;
  const productRef = messagePayload.productRef;
  const productKey = messagePayload.productKey;
  const downloadId = messagePayload.downloadId;

  const fileNameBase = productKey.split('/').slice(-1)[0];
  const fileNameNoExtension = fileNameBase.split('.').slice(0, -1)[0];
  const destPlain = `${directories.productTemp}/${fileNameBase}.gz`;
  const destUnzipped = `${directories.productTemp}/${fileNameBase}`;
  const convertToFormatBase = `${directories.productTemp}/${fileNameNoExtension}`;

  console.log({ fileNameBase, fileNameNoExtension, destPlain, destUnzipped, convertToFormatBase });

  await streamS3toFileSystem(
    config.get('Buckets.productsBucket'),
    productKey,
    destPlain,
    destUnzipped,
  );

  if (messagePayload.products.includes(fileFormats.GEOJSON.label)) {
    await convertToFormat(fileFormats.GEOJSON, convertToFormatBase);

    const individualRefGeoJson = generateRef(referenceIdLength);

    const productKeyGeoJSON = createProductDownloadKey(
      messagePayload.fipsDetails,
      messagePayload.geoid,
      messagePayload.geoName,
      messagePayload.downloadRef,
      productRef,
      individualRefGeoJson,
    );

    if (!isDryRun) {
      await putFileToS3(
        config.get('Buckets.productsBucket'),
        `${productKeyGeoJSON}.geojson`,
        `${convertToFormatBase}.geojson`,
        'application/geo+json',
        true,
      );
      log.info(`uploaded geoJSON file to S3.  key: ${productKeyGeoJSON}`);

      await queryCreateProductRecord(
        messagePayload.downloadId,
        productRef,
        individualRefGeoJson,
        fileFormats.GEOJSON.extension,
        messagePayload.productOrigin,
        messagePayload.geoid,
        `${productKeyGeoJSON}.geojson`,
      );
      log.info(`created geoJSON product record.  ref: ${individualRefGeoJson}`);
    }
  }

  // file conversion is bound to be problematic for GPKG and SHP
  // dont crash program on account of failures here

  if (messagePayload.products.includes(fileFormats.GPKG.label)) {
    try {
      await convertToFormat(fileFormats.GPKG, convertToFormatBase);
      const individualRefGPKG = generateRef(referenceIdLength);
      const productKeyGPKG = createProductDownloadKey(
        messagePayload.fipsDetails,
        messagePayload.geoid,
        messagePayload.geoName,
        messagePayload.downloadRef,
        productRef,
        individualRefGPKG,
      );

      if (!isDryRun) {
        await putFileToS3(
          config.get('Buckets.productsBucket'),
          `${productKeyGPKG}.gpkg`,
          `${convertToFormatBase}.gpkg`,
          'application/geopackage+sqlite3',
          true,
        );
        log.info(`uploaded GPKG file to S3.  key: ${productKeyGPKG}`);

        await queryCreateProductRecord(
          downloadId,
          productRef,
          individualRefGPKG,
          fileFormats.GPKG.extension,
          messagePayload.productOrigin,
          messagePayload.geoid,
          `${productKeyGPKG}.gpkg`,
        );
        log.info(`created GPKG product record.  ref: ${individualRefGPKG}`);
      }
    } catch (e) {
      log.error(e);
      log.info(`ERROR:  !!! uploading or product record creation failed on GPKG.`);
    }
  }

  if (messagePayload.products.includes(fileFormats.SHP.label)) {
    try {
      await convertToFormat(fileFormats.SHP, convertToFormatBase);
      const individualRefSHP = generateRef(referenceIdLength);
      const productKeySHP = createProductDownloadKey(
        messagePayload.fipsDetails,
        messagePayload.geoid,
        messagePayload.geoName,
        messagePayload.downloadRef,
        productRef,
        individualRefSHP,
      );
      await zipShapefile(convertToFormatBase, productKeySHP);

      if (!isDryRun) {
        await putFileToS3(
          config.get('Buckets.productsBucket'),
          `${productKeySHP}-shp.zip`,
          `${directories.outputDir}/${path.parse(productKeySHP).base}-shp.zip`,
          'application/zip',
          false,
        );
        log.info(`uploaded SHP file to S3  key: ${productKeySHP}`);

        await queryCreateProductRecord(
          downloadId,
          productRef,
          individualRefSHP,
          fileFormats.SHP.extension,
          messagePayload.productOrigin,
          messagePayload.geoid,
          `${productKeySHP}-shp.zip`,
        );
        log.info(`created SHP product record.  ref: ${individualRefSHP}`);
      }
    } catch (e) {
      log.error(e);
      log.info(`ERROR:  !!! uploading or product record creation failed on SHP.`);
    }
  }

  // todo TILES

  // todo cleanup

  log.info('product creation finished');
};
