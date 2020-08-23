// @ts-check

exports.directories = {
  EFSroot: '/files/staging',
  SCRATCHroot: './files/staging',
  outputDir: '/files/staging/output-',
  rawDir: '/files/staging/raw-',
  unzippedDir: '/files/staging/unzipped-',
  tilesDir: './files/staging/tiles-',
  subGeographiesDir: '/files/staging/subGeographies-',
  productTempDir: '/files/staging/productTemp-',
  logDir: '/files/staging/logs-',
};

// length of refs for products / downloads table
exports.referenceIdLength = 8;

// length of logfile entropy
exports.logfileNameLength = 5;

// length of directory id
exports.directoryIdLength = 6;

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

exports.messageTypes = {
  INBOX: 'inbox',
  SORT: 'sort',
  PRODUCT: 'product',
};

exports.zoomLevels = {
  LOW: 4,
  HIGH: 14,
};
