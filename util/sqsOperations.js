// @ts-check
const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const { log } = require('../util/logger');

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
