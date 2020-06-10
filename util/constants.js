// @ts-check

exports.directories = {
  outputDir: './output',
  rawDir: './raw',
  unzippedDir: './unzipped',
  processedDir: './processed',
};

exports.buckets = {
  rawBucket: 'raw-data-po',
  productsBucket: 'data-products-po',
  tilesBucket: 'tile-server-po',
};

exports.referenceIdLength = 8;

exports.sourceTypes = {
  WEBPAGE: 'webpage',
  EMAIL: 'email',
};

exports.dispositions = {
  RECEIVED: 'received',
  VIEWED: 'viewed',
  INQUIRED: 'inquired',
};

exports.fileFormats = {
  NDGEOJSON: { driver: 'GeoJSONSeq', extension: 'ndgeojson' },
  GEOJSON: { driver: 'GeoJSON', extension: 'geojson' },
  GPKG: { driver: 'GPKG', extension: 'gpkg' },
  SHP: { driver: 'ESRI Shapefile', extension: 'shp' },
  TILES: { driver: '', extension: 'pbf' },
};

exports.productOrigins = {
  ORIGINAL: 'original',
  DERIVED: 'derived',
};

exports.s3deleteType = {
  FILE: 'file',
  DIRECTORY: 'directory',
};
