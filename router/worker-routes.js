// @ts-check

const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const config = require('config');
const { processInbox } = require('../processors/processInbox');
const { processSort } = require('../processors/processSort');
const { processProducts } = require('../processors/processProducts');
const { processMessage } = require('../util/sqsOperations');
const { log } = require('../util/logger');

exports.appRouter = async app => {
  //
  app.get('/processInbox', async function (req, res) {
    const inboxQueueUrl = config.get('SQS.inboxQueueUrl');

    try {
      const result = await processMessage(inboxQueueUrl, processInbox);
      return res.json(result);
    } catch (e) {
      log.error(e);
      return res.status(500).send(e.message);
    }
  });

  app.get('/processSort', async function (req, res) {
    // await processSort();
    // return res.json({ ok: 'ok' });

    const sortQueueUrl = config.get('SQS.sortQueueUrl');

    try {
      const result = await processMessage(sortQueueUrl, processSort);
      return res.json(result);
    } catch (e) {
      log.error(e);
      return res.status(500).send(e.message);
    }
  });

  app.get('/processProducts', async function (req, res) {
    // await processProducts();
    // return res.json({ ok: 'ok' });

    const productQueueUrl = config.get('SQS.productQueueUrl');

    try {
      const result = await processMessage(productQueueUrl, processProducts);
      return res.json(result);
    } catch (e) {
      log.error(e);
      return res.status(500).send(e.message);
    }
  });
};
