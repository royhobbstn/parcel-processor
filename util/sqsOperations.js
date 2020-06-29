// @ts-check
const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const { log } = require('./logger');

exports.sendQueueMessage = function (queueUrl, payload) {
  return new Promise((resolve, reject) => {
    const params = {
      MessageAttributes: {},
      MessageBody: JSON.stringify(payload),
      QueueUrl: queueUrl,
    };

    sqs.sendMessage(params, function (err, data) {
      if (err) {
        log.error(err);
        return reject(err);
      } else {
        log.info(`Successfully sent message to queue: ${queueUrl}`);
        return resolve();
      }
    });
  });
};

exports.readMessage = readMessage;

function readMessage(queueUrl) {
  return new Promise((resolve, reject) => {
    const params = {
      MaxNumberOfMessages: 1,
      MessageAttributeNames: ['All'],
      QueueUrl: queueUrl,
    };

    sqs.receiveMessage(params, async function (err, data) {
      if (err) {
        log.error(err);
        return reject('Unable to receive SQS Sort message.');
      } else if (data.Messages) {
        log.info('received message');
        return resolve(data);
      } else {
        return resolve('No messages');
      }
    });
  });
}

exports.deleteMessage = deleteMessage;
function deleteMessage(queueUrl, data) {
  return new Promise((resolve, reject) => {
    const deleteParams = {
      QueueUrl: queueUrl,
      ReceiptHandle: data.Messages[0].ReceiptHandle,
    };

    sqs.deleteMessage(deleteParams, function (err, data) {
      if (err) {
        log.error(err);
        return reject('Unable to delete SQS message.');
      } else {
        log.info('Message Deleted', data);
        return resolve({ success: 'Successfully processed SQS Sort message' });
      }
    });
  });
}

exports.processMessage = async function (queueUrl, messageProcessor) {
  const message = await readMessage(queueUrl);
  if (message === 'No message') {
    return { status: 'no message' };
  }

  await deleteMessage(queueUrl, message);

  messageProcessor(message); // purposely not await.  this happens in background

  return { status: 'Processing new message' };
};
