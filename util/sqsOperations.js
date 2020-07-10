// @ts-check
const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });

exports.sendQueueMessage = function (ctx, queueUrl, payload) {
  ctx.log.info('queueUrl', { queueUrl });
  ctx.log.info('queueMessage', { payload });
  return new Promise((resolve, reject) => {
    const params = {
      MessageAttributes: {},
      MessageBody: JSON.stringify(payload),
      QueueUrl: queueUrl,
    };

    sqs.sendMessage(params, function (err, data) {
      if (err) {
        ctx.log.error(`Error sending message to queue: ${queueUrl}`, {
          err: err.message,
          stack: err.stack,
        });
        return reject(err);
      } else {
        ctx.log.info(`Successfully sent message to queue: ${queueUrl}`);
        return resolve();
      }
    });
  });
};

exports.readMessages = readMessages;

function readMessages(ctx, queueUrl, numberOfMessages) {
  return new Promise((resolve, reject) => {
    const params = {
      MaxNumberOfMessages: numberOfMessages, // max = 10
      MessageAttributeNames: ['All'],
      QueueUrl: queueUrl,
    };

    sqs.receiveMessage(params, async function (err, data) {
      if (err) {
        ctx.log.error('Unable to receive SQS message(s)', { err: err.message, stack: err.stack });
        return reject(new Error('Unable to receive SQS message(s)'));
      } else if (data.Messages) {
        ctx.log.info('sqsResponse', { messages: JSON.stringify(data) });

        ctx.log.info('Received message: ' + queueUrl);
        return resolve(data);
      } else {
        ctx.log.info('Found no message: ' + queueUrl);
        return resolve(null);
      }
    });
  });
}

exports.deleteMessage = function (ctx, deleteParams) {
  return new Promise((resolve, reject) => {
    sqs.deleteMessage(deleteParams, function (err, data) {
      if (err) {
        ctx.log.error('Unable to delete SQS message', { err: err.message, stack: err.stack });
        return reject(new Error('Unable to delete SQS message'));
      } else {
        ctx.log.info('Message Deleted', { deleteParams });
        return resolve({ success: 'Successfully deleted SQS message' });
      }
    });
  });
};

exports.initiateVisibilityHeartbeat = function (ctx, deleteParams, intervalMS, heartbeatSec) {
  const params = {
    ...deleteParams,
    VisibilityTimeout: heartbeatSec,
  };

  // set off Initial so we dont have to wait for first interval
  ctx.log.info('attempting to update initial visibility timeout...');
  sqs.changeMessageVisibility(params, function (err, data) {
    if (err) {
      ctx.log.error('Error updating Visibility Timeout', { error: err, stack: err.stack });
    } else {
      ctx.log.info('Refreshing Visibility Timeout - Initial', { data });
    }
  });

  let interval = setInterval(() => {
    // meant to be non-blocking
    ctx.log.info('attempting to update visibility timeout...');
    sqs.changeMessageVisibility(params, function (err, data) {
      if (err) {
        ctx.log.error('Error updating Visibility Timeout', { error: err, stack: err.stack });
      } else {
        ctx.log.info('Refreshing Visibility Timeout', { data });
      }
    });
  }, intervalMS);
  return interval;
};

exports.getQueueAttributes = function (ctx, queueUrl) {
  return new Promise((resolve, reject) => {
    var params = {
      QueueUrl: queueUrl,
      AttributeNames: ['All'],
    };
    sqs.getQueueAttributes(params, function (err, data) {
      if (err) {
        return reject(err);
      }
      return resolve(data);
    });
  });
};
