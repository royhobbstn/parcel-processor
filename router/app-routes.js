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
const { readMessages, deleteMessage, sendQueueMessage } = require('../util/sqsOperations');

exports.appRouter = async app => {
  //

  app.get('/getLogfile', async function (req, res) {
    const ctx = { log };
    const messageId = req.query.messageId;
    const messageType = req.query.messageType;
    const s3Key = `${messageId}-${messageType}.log`;
    const bucket = config.get('Buckets.logfilesBucket');
    res.set('Content-Type', 'text/plain');
    try {
      const data = await getObject(ctx, bucket, s3Key);
      console.log(data);
      return res.send(data);
    } catch (err) {
      ctx.log.error('Error: ', { error: err.message, stack: err.stack });
      return res.status(404).send(err.message);
    }
  });

  app.post('/replay/viewInboxDlq', async function (req, res) {
    const ctx = { log };
    const payload = req.body;
    const originalQueueUrl = config.get('SQS.inboxQueueUrl');
    return replayDlq(ctx, res, originalQueueUrl, payload);
  });

  app.post('/replay/viewSortDlq', async function (req, res) {
    const ctx = { log };
    const payload = req.body;
    const originalQueueUrl = config.get('SQS.sortQueueUrl');
    return replayDlq(ctx, res, originalQueueUrl, payload);
  });

  app.post('/replay/viewProductDlq', async function (req, res) {
    const ctx = { log };
    const payload = req.body;
    const originalQueueUrl = config.get('SQS.productQueueUrl');
    return replayDlq(ctx, res, originalQueueUrl, payload);
  });

  app.get('/viewInboxDlq', async function (req, res) {
    const ctx = { log };
    const queueUrl = config.get('SQS.inboxQueueUrl') + '-dlq';
    return readDlq(ctx, res, queueUrl);
  });

  app.get('/viewSortDlq', async function (req, res) {
    const ctx = { log };
    const queueUrl = config.get('SQS.sortQueueUrl') + '-dlq';
    return readDlq(ctx, res, queueUrl);
  });

  app.get('/viewProductDlq', async function (req, res) {
    const ctx = { log };
    const queueUrl = config.get('SQS.productQueueUrl') + '-dlq';
    return readDlq(ctx, res, queueUrl);
  });

  app.post('/delete/viewInboxDlq', async function (req, res) {
    const ctx = { log };
    const payload = req.body;
    const queueUrl = config.get('SQS.inboxQueueUrl') + '-dlq';
    return deleteDlq(ctx, res, queueUrl, payload);
  });

  app.post('/delete/viewSortDlq', async function (req, res) {
    const ctx = { log };
    const payload = req.body;
    const queueUrl = config.get('SQS.sortQueueUrl') + '-dlq';
    return deleteDlq(ctx, res, queueUrl, payload);
  });

  app.post('/delete/viewProductDlq', async function (req, res) {
    const ctx = { log };
    const payload = req.body;
    const queueUrl = config.get('SQS.productQueueUrl') + '-dlq';
    return deleteDlq(ctx, res, queueUrl, payload);
  });

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
    return sendSQS(ctx, res, inboxQueueUrl, payload);
  });

  app.post('/sendSortSQS', (req, res) => {
    const ctx = { log };
    const sortQueueUrl = config.get('SQS.sortQueueUrl');
    const payload = req.body;
    return sendSQS(ctx, res, sortQueueUrl, payload);
  });

  app.post('/sendProductSQS', (req, res) => {
    const ctx = { log };
    const productsQueueUrl = config.get('SQS.productQueueUrl');
    const payload = req.body;
    return sendSQS(ctx, res, productsQueueUrl, payload);
  });
};

async function sendSQS(ctx, res, queueUrl, payload) {
  const params = {
    MessageAttributes: {},
    MessageBody: JSON.stringify(payload),
    QueueUrl: queueUrl,
  };

  sqs.sendMessage(params, function (err, data) {
    ctx.log.info('SQS response: ', { data });
    if (err) {
      ctx.log.error(`Unable to send SQS message to queue: ${queueUrl}`, {
        err: err.message,
        stack: err.stack,
      });
      return res.status(500).send(`Unable to send SQS message to queue: ${queueUrl}`);
    } else {
      ctx.log.info(`Successfully sent SQS message to queue: ${queueUrl}`);
      return res.json({ success: `Successfully sent SQS message to queue: ${queueUrl}` });
    }
  });
}

async function replayDlq(ctx, res, originalQueueUrl, payload) {
  const queueUrl = originalQueueUrl + '-dlq';
  const deleteParams = {
    ReceiptHandle: payload.ReceiptHandle,
    QueueUrl: queueUrl,
  };
  try {
    await sendQueueMessage(ctx, originalQueueUrl, JSON.parse(payload.Body));
    await deleteMessage(ctx, deleteParams);
    return res.json({ ok: 'ok' });
  } catch (err) {
    ctx.log.error('Unable to replay SQS Message', { error: err.message, stack: err.stack });
    return res.status(500).send('Unable to replay message.');
  }
}

async function deleteDlq(ctx, res, queueUrl, payload) {
  const deleteParams = {
    ReceiptHandle: payload.ReceiptHandle,
    QueueUrl: queueUrl,
  };
  try {
    await deleteMessage(ctx, deleteParams);
    return res.json({ ok: 'ok' });
  } catch (err) {
    ctx.log.error('Unable to delete SQS Message', { error: err.message, stack: err.stack });
    return res.status(500).send('Unable to delete message.');
  }
}

async function readDlq(ctx, res, queueUrl) {
  const response = {
    messages: [],
  };
  try {
    const result = await readMessages(ctx, queueUrl, 10);
    console.log(result);
    if (result) {
      response.messages = result.Messages;
    }
    return res.json(response);
  } catch (err) {
    ctx.log.error('Error reading DLQ', { error: err.message, stack: err.stack });
    return res.status(500).send('Error reading DLQ');
  }
}
