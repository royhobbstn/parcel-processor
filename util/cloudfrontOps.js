// @ts-check
const AWS = require('aws-sdk');
const cloudfront = new AWS.CloudFront({ apiVersion: '2020-05-31' });
const config = require('config');
const { unwindStack, getTimestamp } = require('./misc');

exports.invalidateFiles = invalidateFiles;

function invalidateFiles(ctx, filenamesArray) {
  ctx.process.push({ name: 'invalidateFiles', timestamp: getTimestamp() });

  const params = {
    DistributionId: config.get('Cloudfront.websiteDistributionId'),
    InvalidationBatch: {
      CallerReference: String(Math.round(new Date().getTime() / 1000)),
      Paths: {
        Quantity: filenamesArray.length,
        Items: filenamesArray,
      },
    },
  };

  return new Promise((resolve, reject) => {
    cloudfront.createInvalidation(params, function (err, data) {
      if (err) {
        ctx.log.error('Error', { error: err.message, stack: err.stack });
        return reject(err);
      } else {
        ctx.log.info('cloudwatch invalidation initiated', { data });
        unwindStack(ctx, 'invalidateFiles');
        return resolve(data);
      }
    });
  });
}
