// @ts-check

const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const fs = require('fs');
const config = require('config');
const axios = require('axios').default;
const {
  acquireConnection,
  getSplittableDownloads,
  getCountiesByState,
  getCountySubdivisionsByState,
  querySourceNames,
  querySourceNameExact,
} = require('../util/wrapQuery');
const {
  searchLogsByType,
  searchLogsByGeoid,
  searchLogsByReference,
  getSQSMessagesByGeoidAndType,
  queryProductByIndividualRef,
  queryProductsByProductRef,
  getProductsByDownloadRef,
  getDownloadsByDownloadRef,
} = require('../util/queries');
const { getObject } = require('../util/s3Operations');
const { log } = require('../util/logger');
const {
  readMessages,
  deleteMessage,
  sendQueueMessage,
  getQueueAttributes,
} = require('../util/sqsOperations');
const { getTaskInfo, runProcessorTask } = require('../util/ecsOperations');
const { getTaskLogs } = require('../util/cloudwatchOps');
const { deleteItem } = require('../util/deleteItems');
const { siteData } = require('../util/siteData');

exports.appRouter = async app => {
  //
  app.post('/deleteSelectedItems', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const items = req.body;
    const output = [];
    const sortedItems = items.sort((a, z) => {
      return a.priority - z.priority;
    });
    try {
      for (let item of sortedItems) {
        const response = await deleteItem(ctx, item);
        output.push(response);
      }
      ctx.log.info('Deletions processed');
      return res.json(output);
    } catch (err) {
      ctx.log.error('Unable to delete item(s)', { error: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
  });

  app.get('/triggerSiteData', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };

    try {
      await siteData(ctx);
      // await siteMap(ctx);
      return res.json({ success: true });
    } catch (err) {
      ctx.log.error('Unable to build site data', { error: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
  });

  app.get('/downloadsByDownloadRef', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };

    const downloadRef = req.query.ref;

    try {
      const products = await getDownloadsByDownloadRef(ctx, downloadRef);
      return res.json(products);
    } catch (err) {
      ctx.log.error('Unable to retrieve downloads', { error: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
  });

  app.get('/productsByDownloadRef', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };

    const downloadRef = req.query.ref;

    try {
      const products = await getProductsByDownloadRef(ctx, downloadRef);
      return res.json(products);
    } catch (err) {
      ctx.log.error('Unable to retrieve products', { error: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
  });

  app.get('/getTaskLogs', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };

    const taskId = req.query.id;

    try {
      const logInfo = await getTaskLogs(ctx, taskId);
      return res.json(logInfo);
    } catch (err) {
      ctx.log.error('Unable to retrieve log information', { error: err.message, stack: err.stack });
      return res.json([{ time: '-----', message: err.message, eventId: '1' }]);
    }
  });

  app.get('/runProcessorTask', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };

    try {
      const taskInfo = await runProcessorTask(ctx);
      return res.json(taskInfo);
    } catch (err) {
      ctx.log.error('Error launching task', { error: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
  });

  app.get('/getTaskInfo', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };

    try {
      const taskInfo = await getTaskInfo(ctx);
      return res.json(taskInfo);
    } catch (err) {
      ctx.log.error('Error fetching task information', { error: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
  });

  app.get('/getQueueStats', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };

    const URL_ROOT = 'https://sqs.us-east-2.amazonaws.com/000009394762/';
    const queueStructure = ['inbox', 'sortByGeography', 'createProducts'];
    const envAbbrev =
      process.env.NODE_ENV === 'development'
        ? '-dev'
        : process.env.NODE_ENV === 'test'
        ? '-test'
        : '';
    const queuesWithEnvs = queueStructure.map(queue => `${queue}${envAbbrev}`);
    queuesWithEnvs.forEach(queue => {
      queuesWithEnvs.push(`${queue}-dlq`);
    });

    const details = await Promise.all(
      queuesWithEnvs.map(queue => getQueueAttributes(ctx, URL_ROOT + queue)),
    );

    const structure = {};

    details.forEach(queueDetails => {
      const attributes = queueDetails.Attributes;
      const queue = attributes.QueueArn.split(':')
        .slice(-1)[0]
        .replace('-test', '')
        .replace('-dev', '');

      structure[queue] = {
        available: attributes.ApproximateNumberOfMessages,
        inFlight: attributes.ApproximateNumberOfMessagesNotVisible,
      };
    });

    return res.json(structure);
  });

  app.get('/searchLogsByType', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const type = req.query.type;
    try {
      const query = await searchLogsByType(ctx, type);
      res.json(query.records);
    } catch (err) {
      ctx.log.error('Error: ', { error: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
  });

  app.get('/searchLogsByGeoid', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const geoid = req.query.geoid;
    try {
      const query = await searchLogsByGeoid(ctx, geoid);
      res.json(query.records);
    } catch (err) {
      ctx.log.error('Error: ', { error: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
  });

  app.get('/searchLogsByReference', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const ref = req.query.ref;
    try {
      const query = await searchLogsByReference(ctx, ref);
      res.json(query.records);
    } catch (err) {
      ctx.log.error('Error: ', { error: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
  });

  app.get('/getLogfile', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const messageId = req.query.messageId;
    const messageType = req.query.messageType;
    const s3Key = `${messageId}-${messageType}.log`;
    const bucket = config.get('Buckets.logfilesBucket');
    res.set('Content-Type', 'text/plain');
    try {
      const data = await getObject(ctx, bucket, s3Key);
      return res.send(data);
    } catch (err) {
      ctx.log.error('Error: ', { error: err.message, stack: err.stack });
      return res.status(404).send(err.message);
    }
  });

  app.post('/replay/viewInboxDlq', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const payload = req.body;
    const originalQueueUrl = config.get('SQS.inboxQueueUrl');
    return replayDlq(ctx, res, originalQueueUrl, payload);
  });

  app.post('/replay/viewSortDlq', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const payload = req.body;
    const originalQueueUrl = config.get('SQS.sortQueueUrl');
    return replayDlq(ctx, res, originalQueueUrl, payload);
  });

  app.post('/replay/viewProductDlq', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const payload = req.body;
    const originalQueueUrl = config.get('SQS.productQueueUrl');
    return replayDlq(ctx, res, originalQueueUrl, payload);
  });

  app.get('/viewInboxDlq', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const queueUrl = config.get('SQS.inboxQueueUrl') + '-dlq';
    return readDlq(ctx, res, queueUrl);
  });

  app.get('/viewSortDlq', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const queueUrl = config.get('SQS.sortQueueUrl') + '-dlq';
    return readDlq(ctx, res, queueUrl);
  });

  app.get('/viewProductDlq', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const queueUrl = config.get('SQS.productQueueUrl') + '-dlq';
    return readDlq(ctx, res, queueUrl);
  });

  app.post('/delete/viewInboxDlq', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const payload = req.body;
    const queueUrl = config.get('SQS.inboxQueueUrl') + '-dlq';
    return deleteDlq(ctx, res, queueUrl, payload);
  });

  app.post('/delete/viewSortDlq', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const payload = req.body;
    const queueUrl = config.get('SQS.sortQueueUrl') + '-dlq';
    return deleteDlq(ctx, res, queueUrl, payload);
  });

  app.post('/delete/viewProductDlq', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const payload = req.body;
    const queueUrl = config.get('SQS.productQueueUrl') + '-dlq';
    return deleteDlq(ctx, res, queueUrl, payload);
  });

  app.get('/queryStatFiles', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
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
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
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
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const geoid = req.query.geoid;
    try {
      await acquireConnection(ctx);

      let rows;
      if (
        geoid === '09' ||
        geoid === '23' ||
        geoid === '25' ||
        geoid === '44' ||
        geoid === '33' ||
        geoid === '50'
      ) {
        // if new england, return county subdivisions
        rows = await getCountySubdivisionsByState(ctx, geoid);
      } else {
        // everywhere else return counties
        rows = await getCountiesByState(ctx, geoid);
      }

      return res.json(rows);
    } catch (err) {
      return res.status(500).send(err.message);
    }
  });

  app.get('/querySources', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const sourceName = decodeURIComponent(req.query.name);
    try {
      await acquireConnection(ctx);
      const rows = await querySourceNames(ctx, sourceName);
      return res.json(rows);
    } catch (err) {
      return res.status(500).send(err.message);
    }
  });

  app.get('/checkSourceExists', async function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const sourceName = decodeURIComponent(req.query.sourceName);
    try {
      await acquireConnection(ctx);
      const rows = await querySourceNameExact(ctx, sourceName);
      if (rows.length) {
        return res.json({ status: true });
      } else {
        return res.json({ status: false });
      }
    } catch (err) {
      return res.status(500).send(err.message);
    }
  });

  app.get('/proxyHeadRequest', function (req, res) {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
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
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    let inboxQueueUrl = config.get('SQS.inboxQueueUrl');
    const envOverride = req.query.env;
    if (envOverride) {
      inboxQueueUrl = readSqsConfig(envOverride, 'inboxQueueUrl');
    }
    const payload = req.body;
    return sendSQS(ctx, res, inboxQueueUrl, payload);
  });

  app.post('/sendSortSQS', (req, res) => {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    let sortQueueUrl = config.get('SQS.sortQueueUrl');
    const envOverride = req.query.env;
    if (envOverride) {
      sortQueueUrl = readSqsConfig(envOverride, 'sortQueueUrl');
    }
    const payload = req.body;
    return sendSQS(ctx, res, sortQueueUrl, payload);
  });

  app.post('/sendProductSQS', (req, res) => {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    let productQueueUrl = config.get('SQS.productQueueUrl');
    const envOverride = req.query.env;
    if (envOverride) {
      productQueueUrl = readSqsConfig(envOverride, 'productQueueUrl');
    }
    const payload = req.body;
    return sendSQS(ctx, res, productQueueUrl, payload);
  });

  app.get('/getSQSMessageBody', async (req, res) => {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const messageType = req.query.type;
    const geoid = req.query.geoid;
    try {
      const rows = await getSQSMessagesByGeoidAndType(ctx, messageType, geoid);
      return res.json(rows);
    } catch (err) {
      ctx.log.error('Error', { err: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
  });

  app.get('/byProductIndividualRef', async (req, res) => {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const productIndividualRef = req.query.ref;
    console.log({ productIndividualRef });
    try {
      const rows = await queryProductByIndividualRef(ctx, productIndividualRef);
      return res.json(rows);
    } catch (err) {
      ctx.log.error('Error', { err: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
  });

  app.get('/byProductRef', async (req, res) => {
    const ctx = { log, process: [], timeBank: {}, timeStack: [] };
    const productRef = req.query.ref;
    console.log({ productRef });
    try {
      const rows = await queryProductsByProductRef(ctx, productRef);
      return res.json(rows);
    } catch (err) {
      ctx.log.error('Error', { err: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    }
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
    const result = await readMessages(ctx, queueUrl, 1);
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

function readSqsConfig(env, queueConfig) {
  const config = fs.readFileSync(`./config/${env}.json`, 'utf8');
  return JSON.parse(config).SQS[queueConfig];
}
