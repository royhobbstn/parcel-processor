// @ts-check

exports.directories = {
  outputDir: './staging/output',
  rawDir: './staging/raw',
  unzippedDir: './staging/unzipped',
  processedDir: './staging/processed',
  subGeographiesDir: './staging/subGeographies',
  productTempDir: './staging/productTemp',
  logDir: './staging/logs',
};

// length of refs for products / downloads table
exports.referenceIdLength = 8;

// logfiles
exports.logfileNameLength = 5;

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
