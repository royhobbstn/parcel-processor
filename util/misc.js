// @ts-check
const { execSync } = require('child_process');
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

exports.getStatus = async function (ctx) {
  const applications = ['tippecanoe', 'ogr2ogr', 'aws'];
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

  try {
    const output = execSync(`df -h . | tr -s ' ' ',' | jq -nR '[ 
      ( input | split(",") ) as $keys | 
      ( inputs | split(",") ) as $vals | 
      [ [$keys, $vals] | 
      transpose[] | 
      {key:.[0],value:.[1]} ] | 
      from_entries ]'`);
    const parsed = JSON.parse(output.toString());
    status['disk'] = parsed;
    ctx.log.info(`disk check passed`);
  } catch (e) {
    ctx.log.info(`disk check failed`);
  }

  return status;
};
