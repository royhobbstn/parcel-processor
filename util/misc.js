// @ts-check
const present = require('present');
const { execSync } = require('child_process');
const { sourceTypes } = require('./constants');

exports.unwindStack = unwindStack;

function unwindStack(ctx, itemToRemove) {
  for (const [index, value] of ctx.process.entries()) {
    if (value.name === itemToRemove) {
      const elapsed = present() - value.timestamp;
      ctx.timeStack.push({ fn: itemToRemove, duration: elapsed });
      if (!ctx.timeBank[itemToRemove]) {
        ctx.timeBank[itemToRemove] = elapsed;
      } else {
        ctx.timeBank[itemToRemove] += elapsed;
      }
      ctx.log.info(`${itemToRemove} finished in ${Math.floor(elapsed)} ms`);
      ctx.process.splice(index, 1); // mutation in place
      break;
    }
  }
}

exports.getTimestamp = getTimestamp;

function getTimestamp() {
  return present();
}

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

exports.initiateFreeMemoryQuery = function (ctx, seconds) {
  ctx.process.push({ name: 'initiateFreeMemoryQuery', timestamp: getTimestamp() });

  let interval = setInterval(() => {
    try {
      const output = execSync('free -mh');
      ctx.log.info('Mem: ' + output.toString());
    } catch (e) {
      ctx.log.info(`mem check failed`);
    }
  }, seconds * 1000);
  unwindStack(ctx, 'initiateFreeMemoryQuery');
  return interval;
};

exports.initiateDiskSpaceQuery = function (ctx, seconds) {
  ctx.process.push({ name: 'initiateDiskSpaceQuery', timestamp: getTimestamp() });

  let interval = setInterval(() => {
    try {
      const output = execSync('df -h');
      ctx.log.info('HD: ' + output.toString());
    } catch (e) {
      ctx.log.info(`disk space check failed`);
    }
  }, seconds * 1000);
  unwindStack(ctx, 'initiateDiskSpaceQuery');
  return interval;
};

exports.getStatus = async function (ctx) {
  ctx.process.push({ name: 'getStatus', timestamp: getTimestamp() });

  const applications = ['tippecanoe', 'tile-join', 'ogr2ogr', 'aws', 'go'];
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

  unwindStack(ctx, 'getStatus');
  return status;
};

exports.sleep = function (ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
};

exports.initiateProgressHeartbeat = function (ctx, seconds) {
  ctx.process.push({ name: 'initiateProgressHeartbeat', timestamp: getTimestamp() });

  const interval = setInterval(() => {
    ctx.log.info(
      `still processing: ${ctx.process
        .slice(-10)
        .reverse()
        .map(d => d.name)
        .join(', ')}`,
    );
  }, seconds * 1000);

  unwindStack(ctx, 'initiateProgressHeartbeat');
  return interval;
};

exports.isNewEngland = stateFips => {
  return (
    stateFips === '09' ||
    stateFips === '23' ||
    stateFips === '25' ||
    stateFips === '44' ||
    stateFips === '33' ||
    stateFips === '50'
  );
};
