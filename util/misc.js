// @ts-check

const { sourceTypes } = require('./constants');

exports.getSourceType = function (ctx, sourceNameInput) {
  if (
    sourceNameInput.includes('http://') ||
    sourceNameInput.includes('https://') ||
    sourceNameInput.includes('ftp://')
  ) {
    ctx.log.info('Determined to be a WEBPAGE source');
    return sourceTypes.WEBPAGE;
  } else if (sourceNameInput.includes('@') && sourceNameInput.includes('.')) {
    // can probably validate better than this
    ctx.log.info('Determined to be an EMAIL source');
    return sourceTypes.EMAIL;
  } else {
    throw new Error('Could not determine source type');
  }
};
