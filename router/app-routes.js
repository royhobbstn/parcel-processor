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
    const geoid = req.query.geoid;
    await acquireConnection();
    const rows = await getSplittableDownloads(geoid);
    return res.json(rows);
  });

  app.get('/proxyS3File', async function (req, res) {
    const key = decodeURIComponent(req.query.key);
    const bucket = decodeURIComponent(req.query.bucket);
    const data = await getObject(bucket, key);
    return res.json(data);
  });

  app.get('/getSubGeographies', async function (req, res) {
    const geoid = req.query.geoid;
    await acquireConnection();
    const rows = await getCountiesByState(geoid);
    return res.json(rows);
  });

  app.get('/querySources', async function (req, res) {
    const sourceName = decodeURIComponent(req.query.name);
    await acquireConnection();
    const rows = await querySourceNames(sourceName);
    return res.json(rows);
  });

  app.get('/proxyHeadRequest', function (req, res) {
    const url = decodeURIComponent(req.query.url);
    axios
      .head(url)
      .then(function (response) {
        if (response.status === 200) {
          return true;
        } else {
          return false;
        }
      })
      .catch(function (error) {
        console.log(error);
        return false;
      })
      .then(function (result) {
        return res.json({ status: result });
      });
  });

  app.post('/sendInboxSQS', async function (req, res) {
    const inboxQueueUrl = config.get('SQS.inboxQueueUrl');

    const payload = req.body;

    const params = {
      MessageAttributes: {},
      MessageBody: JSON.stringify(payload),
      QueueUrl: inboxQueueUrl,
    };

    sqs.sendMessage(params, function (err, data) {
      if (err) {
        log.error(err);
        return res.status(500).send('Unable to send SQS message.');
      } else {
        return res.json({ success: 'Successfully sent SQS message' });
      }
    });
  });

  app.post('/sendSortSQS', async function (req, res) {
    const sortQueueUrl = config.get('SQS.sortQueueUrl');

    const payload = req.body;

    const params = {
      MessageAttributes: {},
      MessageBody: JSON.stringify(payload),
      QueueUrl: sortQueueUrl,
    };

    sqs.sendMessage(params, function (err, data) {
      if (err) {
        log.error(err);
        return res.status(500).send('Unable to send SQS message.');
      } else {
        return res.json({ success: 'Successfully sent SQS message' });
      }
    });
  });
};
