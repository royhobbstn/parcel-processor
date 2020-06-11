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

// length of refs for products / downloads table
exports.referenceIdLength = 8;

// prefix to flag that file needs to be gzipped in a separate process
exports.tileInfoPrefix = '_po_';

// parcel-outlet guaranteed unique feature id
exports.idPrefix = '__po_id';

// parcel-outlet feature cluster id (refers to file where tile attributes are stored)
exports.clusterPrefix = '__po_cl';

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
