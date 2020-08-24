// @ts-check
const { execSync } = require('child_process');
const { sourceTypes } = require('./constants');

exports.unwindStack = unwindStack;

function unwindStack(arr, itemToRemove) {
  const index = arr.lastIndexOf(itemToRemove);
  arr.splice(index, 1); // mutation in place
}

exports.getSourceType = function (ctx, sourceNameInput) {
  ctx.process.push('getSourceType');

  if (
    sourceNameInput.includes('http://') ||
    sourceNameInput.includes('https://') ||
    sourceNameInput.includes('ftp://')
  ) {
    ctx.log.info('Determined to be a WEBPAGE source');
    unwindStack(ctx.process, 'getSourceType');
    return sourceTypes.WEBPAGE;
  } else if (sourceNameInput.includes('@') && sourceNameInput.includes('.')) {
    // can probably validate better than this
    ctx.log.info('Determined to be an EMAIL source');
    unwindStack(ctx.process, 'getSourceType');
    return sourceTypes.EMAIL;
  } else {
    throw new Error('Could not determine source type');
  }
};

exports.initiateFreeMemoryQuery = function (ctx) {
  ctx.process.push('initiateFreeMemoryQuery');

  let interval = setInterval(() => {
    try {
      const output = execSync('free -mh');
      ctx.log.info('Mem: ' + output.toString());
    } catch (e) {
      ctx.log.info(`mem check failed`);
    }
  }, 60000);
  unwindStack(ctx.process, 'initiateFreeMemoryQuery');
  return interval;
};

exports.getStatus = async function (ctx) {
  ctx.process.push('getStatus');

  const applications = ['tippecanoe', 'tile-join', 'ogr2ogr', 'aws'];
  const status = {};

  for (let appName of applications) {
    try {
      execSync('command -v ' + appName);
      status[appName] = 'ok';
      ctx.log.info(`${appName}: passed`);
    } catch (e) {
      status[appName] = 'failed';
      ctx.log.info(`${appName}: failed`);
    }
  }

  unwindStack(ctx.process, 'getStatus');
  return status;
};

exports.sleep = function (ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
};

exports.initiateProgressHeartbeat = function (ctx, seconds) {
  ctx.process.push('initiateProgressHeartbeat');

  const interval = setInterval(() => {
    ctx.log.info(`still processing: ${ctx.process.slice(-10).reverse().join(', ')}`);
  }, seconds * 1000);

  unwindStack(ctx.process, 'initiateProgressHeartbeat');
  return interval;
};
