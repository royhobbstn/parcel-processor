// @ts-check

exports.directories = {
  outputDir: './output',
  rawDir: './raw',
  unzippedDir: './unzipped',
  processedDir: './processed',
  subGeographiesDir: './processors/subGeographies',
  productTemp: './processors/productTemp',
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
  NDGEOJSON: { driver: 'GeoJSONSeq', extension: 'ndgeojson', label: 'NDGEOJSON' },
  GEOJSON: { driver: 'GeoJSON', extension: 'geojson', label: 'GEOJSON' },
  GPKG: { driver: 'GPKG', extension: 'gpkg', label: 'GPKG' },
  SHP: { driver: 'ESRI Shapefile', extension: 'shp', label: 'SHP' },
  TILES: { driver: '', extension: 'pbf', label: 'TILES' },
};

exports.productOrigins = {
  ORIGINAL: 'original',
  DERIVED: 'derived',
};

exports.s3deleteType = {
  FILE: 'file',
  DIRECTORY: 'directory',
};

exports.modes = {
  FULL_RUN: { input: 'full', label: 'FULL_RUN' },
  DRY_RUN: { input: 'dry', label: 'DRY RUN' },
};
