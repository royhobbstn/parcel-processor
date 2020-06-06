// application constants

exports.outputDir = './output';
exports.rawDir = './raw';
exports.unzippedDir = './unzipped';
exports.processedDir = './processed';

exports.rawBucket = 'raw-data-po';
exports.productsBucket = 'data-products-po';

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
};

exports.productOrigins = {
  ORIGINAL: 'original',
  DERIVED: 'derived',
};
