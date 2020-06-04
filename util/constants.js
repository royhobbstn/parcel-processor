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
