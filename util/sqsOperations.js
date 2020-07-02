// @ts-check
const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });

exports.sendQueueMessage = function (ctx, queueUrl, payload) {
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

exports.readMessage = readMessage;

function readMessage(ctx, queueUrl) {
  return new Promise((resolve, reject) => {
    const params = {
      MaxNumberOfMessages: 1,
      MessageAttributeNames: ['All'],
      QueueUrl: queueUrl,
    };

    sqs.receiveMessage(params, async function (err, data) {
      if (err) {
        ctx.log.error('Unable to receive SQS Sort message', { err: err.message, stack: err.stack });
        return reject(new Error('Unable to receive SQS Sort message'));
      } else if (data.Messages) {
        ctx.log.info('Received message');
        return resolve(data);
      } else {
        ctx.log.info('Found no messages');
        return resolve('');
      }
    });
  });
}

exports.deleteMessage = deleteMessage;

function deleteMessage(ctx, queueUrl, data) {
  return new Promise((resolve, reject) => {
    const deleteParams = {
      QueueUrl: queueUrl,
      ReceiptHandle: data.Messages[0].ReceiptHandle,
    };

    sqs.deleteMessage(deleteParams, function (err, data) {
      if (err) {
        ctx.log.error('Unable to delete SQS message', { err: err.message, stack: err.stack });
        return reject(new Error('Unable to delete SQS message'));
      } else {
        ctx.log.info('Message Deleted', data);
        return resolve({ success: 'Successfully processed SQS Sort message' });
      }
    });
  });
}

exports.processMessage = async function (ctx, queueUrl) {
  const message = await readMessage(ctx, queueUrl);
  if (message) {
    await deleteMessage(ctx, queueUrl, message);
  }
  return message;
};
