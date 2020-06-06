const path = require('path');
const {
  outputDir,
  fileFormats,
  referenceIdLength,
  productsBucket,
  productOrigins,
} = require('./constants');
const { uploadProductFiles, createProductDownloadKey } = require('./wrappers/wrapS3');
const { putFileToS3 } = require('./primitives/s3Operations');
const { queryCreateProductRecord } = require('./primitives/queries');
const { convertToFormat } = require('./processGeoFile');
const { generateRef } = require('./crypto');
const { zipShapefile } = require('./filesystemUtil');

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
    productsBucket,
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
      productsBucket,
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
      productsBucket,
      `${productKeySHP}-shp.zip`,
      `${outputDir}/${path.parse(productKeySHP).base}-shp.zip`,
      'application/zip',
      false,
    );
  } catch (e) {
    console.error(e);
  }
};
