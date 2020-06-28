// @ts-check

const { sourceTypes } = require('./constants');
const { log } = require('./logger');

exports.getSourceType = function (sourceNameInput) {
  if (
    sourceNameInput.includes('http://') ||
    sourceNameInput.includes('https://') ||
    sourceNameInput.includes('ftp://')
  ) {
    log.info('\nDetermined to be a WEBPAGE source.\n');
    return sourceTypes.WEBPAGE;
  } else if (sourceNameInput.includes('@') && sourceNameInput.includes('.')) {
    // can probably validate better than this
    log.info('\nDetermined to be an EMAIL source.\n');
    return sourceTypes.EMAIL;
  } else {
    throw new Error('Could not determine source type.');
  }
};
