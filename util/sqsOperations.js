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
  // note that for small scale operations - numberOfMessages is worthless and you should just set it to 1.  see AWS docs.
  return new Promise((resolve, reject) => {
    const params = {
      MaxNumberOfMessages: numberOfMessages,
      MessageAttributeNames: ['All'],
      QueueUrl: queueUrl,
    };

    ctx.log.info('numberOfMessagesRequested', { numberOfMessages });

    sqs.receiveMessage(params, async function (err, data) {
      if (err) {
        ctx.log.error('Unable to receive SQS message(s)', { err: err.message, stack: err.stack });
        return reject(new Error('Unable to receive SQS message(s)'));
      } else if (data.Messages) {
        ctx.log.info('sqsResponse', { data });

        ctx.log.info('Received message(s)');
        return resolve(data);
      } else {
        ctx.log.info('Found no message(s)');
        return resolve('');
      }
    });
  });
}

exports.deleteMessage = function (ctx) {
  return new Promise((resolve, reject) => {
    sqs.deleteMessage(ctx.deleteParams, function (err, data) {
      if (err) {
        ctx.log.error('Unable to delete SQS message', { err: err.message, stack: err.stack });
        return reject(new Error('Unable to delete SQS message'));
      } else {
        ctx.log.info('Message Deleted', { deleteParams: ctx.deleteParams });
        return resolve({ success: 'Successfully deleted SQS message' });
      }
    });
  });
};

function attachDeleteParams(ctx, queueUrl, data) {
  const deleteParams = {
    QueueUrl: queueUrl,
    ReceiptHandle: data.Messages[0].ReceiptHandle,
  };
  ctx.deleteParams = deleteParams;
}

exports.processMessage = async function (ctx, queueUrl) {
  const message = await readMessages(ctx, queueUrl, 1);
  if (message) {
    // store delete payload on ctx to be used later
    attachDeleteParams(ctx, queueUrl, message);
  }
  return message;
};

exports.initiateVisibilityHeartbeat = function (ctx, intervalMS, heartbeatSec) {
  const params = {
    ...ctx.deleteParams,
    VisibilityTimeout: heartbeatSec,
  };

  // set off Initial so we dont have to wait for first interval
  sqs.changeMessageVisibility(params, function (err, data) {
    if (err) {
      ctx.log.error('Error updating Visibility Timeout', { error: err, stack: err.stack });
    } else {
      ctx.log.info('Refreshing Visibility Timeout', { data });
    }
  });

  let interval = setInterval(() => {
    // meant to be non-blocking
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
