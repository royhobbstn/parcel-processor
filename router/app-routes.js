// @ts-check

const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const config = require('config');
const axios = require('axios').default;
const {
  acquireConnection,
  getSplittableDownloads,
  getCountiesByState,
  querySourceNames,
} = require('../util/wrapQuery');
const { getObject } = require('../util/s3Operations');
const { log } = require('../util/logger');

exports.appRouter = async app => {
  //
  app.get('/queryStatFiles', async function (req, res) {
    const ctx = { log };
    const geoid = req.query.geoid;
    try {
      await acquireConnection(ctx);
      const rows = await getSplittableDownloads(ctx, geoid);
      return res.json(rows);
    } catch (err) {
      return res.status(500).send(err.message);
    }
  });

  app.get('/proxyS3File', async function (req, res) {
    const ctx = { log };
    const key = decodeURIComponent(req.query.key);
    const bucket = decodeURIComponent(req.query.bucket);
    try {
      const data = await getObject(ctx, bucket, key);
      return res.json(data);
    } catch (err) {
      return res.status(500).send(err.message);
    }
  });

  app.get('/getSubGeographies', async function (req, res) {
    const ctx = { log };
    const geoid = req.query.geoid;
    try {
      await acquireConnection(ctx);
      const rows = await getCountiesByState(ctx, geoid);
      return res.json(rows);
    } catch (err) {
      return res.status(500).send(err.message);
    }
  });

  app.get('/querySources', async function (req, res) {
    const ctx = { log };
    const sourceName = decodeURIComponent(req.query.name);
    try {
      await acquireConnection(ctx);
      const rows = await querySourceNames(ctx, sourceName);
      return res.json(rows);
    } catch (err) {
      return res.status(500).send(err.message);
    }
  });

  app.get('/proxyHeadRequest', function (req, res) {
    const ctx = { log };
    const url = decodeURIComponent(req.query.url);
    axios
      .head(url)
      .then(function (response) {
        ctx.log.info('Status: ' + response.status);
        if (response.status === 200) {
          return true;
        } else {
          return false;
        }
      })
      .catch(err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        ctx.log.info(`Unable to proxy: ${url}`);
        return false;
      })
      .then(function (result) {
        return res.json({ status: result });
      });
  });

  app.post('/sendInboxSQS', async function (req, res) {
    const ctx = { log };

    const inboxQueueUrl = config.get('SQS.inboxQueueUrl');

    const payload = req.body;

    const params = {
      MessageAttributes: {},
      MessageBody: JSON.stringify(payload),
      QueueUrl: inboxQueueUrl,
    };

    sqs.sendMessage(params, (err, data) => {
      ctx.log.info('SQS response: ', { data });
      if (err) {
        ctx.log.error(`Unable to send SQS message to queue: ${inboxQueueUrl}`, {
          err: err.message,
          stack: err.stack,
        });
        return res.status(500).send(`Unable to send SQS message to queue: ${inboxQueueUrl}`);
      } else {
        ctx.log.info(`Successfully sent SQS message to queue: ${inboxQueueUrl}`);
        return res.json({ success: `Successfully sent SQS message to queue: ${inboxQueueUrl}` });
      }
    });
  });

  app.post('/sendSortSQS', (req, res) => {
    const ctx = { log };

    const sortQueueUrl = config.get('SQS.sortQueueUrl');

    const payload = req.body;

    const params = {
      MessageAttributes: {},
      MessageBody: JSON.stringify(payload),
      QueueUrl: sortQueueUrl,
    };

    sqs.sendMessage(params, function (err, data) {
      ctx.log.info('SQS response: ', { data });
      if (err) {
        ctx.log.error(`Unable to send SQS message to queue: ${sortQueueUrl}`, {
          err: err.message,
          stack: err.stack,
        });
        return res.status(500).send(`Unable to send SQS message to queue: ${sortQueueUrl}`);
      } else {
        ctx.log.info(`Successfully sent SQS message to queue: ${sortQueueUrl}`);
        return res.json({ success: `Successfully sent SQS message to queue: ${sortQueueUrl}` });
      }
    });
  });

  app.post('/sendProductSQS', (req, res) => {
    const ctx = { log };

    const productsQueueUrl = config.get('SQS.productQueueUrl');

    const payload = req.body;

    const params = {
      MessageAttributes: {},
      MessageBody: JSON.stringify(payload),
      QueueUrl: productsQueueUrl,
    };

    sqs.sendMessage(params, function (err, data) {
      ctx.log.info('SQS response: ', { data });
      if (err) {
        ctx.log.error(`Unable to send SQS message to queue: ${productsQueueUrl}`, {
          err: err.message,
          stack: err.stack,
        });
        return res.status(500).send(`Unable to send SQS message to queue: ${productsQueueUrl}`);
      } else {
        ctx.log.info(`Successfully sent SQS message to queue: ${productsQueueUrl}`);
        return res.json({ success: `Successfully sent SQS message to queue: ${productsQueueUrl}` });
      }
    });
  });
};
