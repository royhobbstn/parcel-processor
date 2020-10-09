// @ts-check
const AWS = require('aws-sdk');
const cloudwatchlogs = new AWS.CloudWatchLogs({ apiVersion: '2014-03-28', region: 'us-east-2' });
const config = require('config');
const { unwindStack, getTimestamp } = require('./misc');

exports.getTaskLogs = getTaskLogs;

function getTaskLogs(ctx, taskId) {
  ctx.process.push({ name: 'getTaskLogs', timestamp: getTimestamp() });

  return new Promise((resolve, reject) => {
    const logGroupName = config.get('ECS.logGroupName');

    const params = {
      logGroupName: `/${logGroupName}`,
      logStreamNames: [`${logGroupName}/${taskId}`],
    };
    cloudwatchlogs.filterLogEvents(params, function (err, data) {
      if (err) {
        return reject(err);
      }

      const mapped = data.events.map(event => {
        return {
          time: new Date(event.timestamp).toLocaleTimeString(),
          message: event.message,
          eventId: event.eventId,
        };
      });

      unwindStack(ctx, 'getTaskLogs');
      return resolve(mapped);
    });
  });
}
