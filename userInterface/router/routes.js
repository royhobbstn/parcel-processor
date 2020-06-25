// @ts-check

const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const { v4: uuidv4 } = require('uuid');
const config = require('config');
const axios = require('axios').default;
const {
  acquireConnection,
  getSplittableDownloads,
  getCountiesByState,
  querySourceNames,
} = require('../../util/wrappers/wrapQuery');
const { getObject } = require('../../util/primitives/s3Operations');
const { queryHealth } = require('../../util/primitives/queries');
const { processInbox } = require('../processors/processInbox');

exports.appRouter = async app => {
  //
  app.get('/databaseKeepAlive', function (req, res) {
    const pr = queryHealth(); // no await on purpose
    return res.json({ ok: 'ok' });
  });

  app.get('/fetchEnv', async function (req, res) {
    await acquireConnection();
    return res.json({ env: process.env.NODE_ENV });
  });

  app.get('/queryStatFiles', async function (req, res) {
    const geoid = req.query.geoid;
    console.log({ geoid });
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
    //
    const inboxQueueUrl = config.get('SQS.inboxQueueUrl');

    const payload = req.body;

    const params = {
      MessageAttributes: {},
      MessageBody: JSON.stringify(payload),
      QueueUrl: inboxQueueUrl,
    };

    sqs.sendMessage(params, function (err, data) {
      if (err) {
        console.error(err);
        return res.status(500).send('Unable to send SQS message.');
      } else {
        return res.json({ success: 'Successfully sent SQS message' });
      }
    });
  });

  app.post('/sendSortSQS', async function (req, res) {
    const sortQueueUrl = config.get('SQS.sortQueueUrl');

    const payload = req.body;

    // {
    //   selectedFieldKey: 'county',
    //   selectedDownload: {
    //     geoid: '15',
    //     geoname: 'Hawaii',
    //     source_name: 'http://planning.hawaii.gov/gis/download-gis-data/',
    //     source_type: 'webpage',
    //     download_id: 3,
    //     download_ref: '5fe3a581',
    //     product_id: 11,
    //     product_ref: '65aa145b',
    //     last_checked: '2020-06-21 16:11:24',
    //     product_key: '15-Hawaii/000-Hawaii/5fe3a581-65aa145b-15-Hawaii.ndgeojson',
    //     original_filename: 'tmk_state.shp.zip'
    //   },
    //   modalStatsObj: {
    //     missingAttributes: [],
    //     missingGeoids: [ '15005' ],
    //     countOfPossible: 5,
    //     countOfUniqueGeoids: 4,
    //     attributesUsingSameGeoid: [],
    //     mapping: {
    //       Hawaii: '15001',
    //       Honolulu: '15003',
    //       Kauai: '15007',
    //       Maui: '15009'
    //     }
    //   }
    // }

    const params = {
      MessageAttributes: {},
      MessageBody: JSON.stringify(payload),
      QueueUrl: sortQueueUrl,
    };

    sqs.sendMessage(params, function (err, data) {
      if (err) {
        console.error(err);
        return res.status(500).send('Unable to send SQS message.');
      } else {
        return res.json({ success: 'Successfully sent SQS message' });
      }
    });
  });
  //
  app.get('/processInbox', async function (req, res) {
    const inboxQueueUrl = config.get('SQS.inboxQueueUrl');

    readMessage(inboxQueueUrl, processInbox)
      .then(response => {
        return res.json(response);
      })
      .catch(err => {
        return res.status(500).send(err);
      });
  });

  app.get('/processSort', async function (req, res) {
    const sortQueueUrl = config.get('SQS.sortQueueUrl');

    const processMessage = data => {
      console.log('processing sort todo');
    };

    readMessage(sortQueueUrl, processMessage)
      .then(response => {
        return res.json(response);
      })
      .catch(err => {
        return res.status(500).send(err);
      });
  });

  app.get('/processProduct', async function (req, res) {
    const productQueueUrl = config.get('SQS.productQueueUrl');

    const processMessage = data => {
      console.log('processing product todo');
    };

    readMessage(productQueueUrl, processMessage)
      .then(response => {
        return res.json(response);
      })
      .catch(err => {
        return res.status(500).send(err);
      });
  });
};

function readMessage(queueUrl, callback) {
  return new Promise((resolve, reject) => {
    const params = {
      MaxNumberOfMessages: 1,
      MessageAttributeNames: ['All'],
      QueueUrl: queueUrl,
    };

    sqs.receiveMessage(params, async function (err, data) {
      if (err) {
        console.error(err);
        return reject('Unable to receive SQS Sort message.');
      } else if (data.Messages) {
        console.log('received message');
        console.log({ data });

        try {
          await callback(data);
        } catch (e) {
          console.error('Error handling message.');
          console.error(e);
        }

        try {
          await deleteMessage(queueUrl, data);
        } catch (e) {
          reject(e);
        }

        return resolve({ success: 'Message processed successfully.' });
      } else {
        try {
          await deleteMessage(queueUrl, data);
        } catch (e) {
          reject(e);
        }
        return resolve({ success: 'There were no new messages.' });
      }
    });
  });
}

function deleteMessage(queueUrl, data) {
  return new Promise((resolve, reject) => {
    const deleteParams = {
      QueueUrl: queueUrl,
      ReceiptHandle: data.Messages[0].ReceiptHandle,
    };

    sqs.deleteMessage(deleteParams, function (err, data) {
      if (err) {
        console.error(err);
        reject('Unable to delete SQS message.');
      } else {
        console.log('Message Deleted', data);
        resolve({ success: 'Successfully processed SQS Sort message' });
      }
    });
  });
}
