const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const { v4: uuidv4 } = require('uuid');
const config = require('config');
const {
  acquireConnection,
  getSplittableDownloads,
  getCountiesByState,
  querySourceNames,
} = require('../../util/wrappers/wrapQuery');
const { getObject } = require('../../util/primitives/s3Operations');

exports.appRouter = async app => {
  //
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

  app.post('/sendSortSQS', async function (req, res) {
    const sortQueueUrl = config.get('SQS.sortQueueUrl');

    const payload = req.body;
    console.log(payload);

    var params = {
      MessageAttributes: {
        geoid: {
          DataType: 'String',
          StringValue: payload.selectedDownload.geoid,
        },
        geoname: {
          DataType: 'String',
          StringValue: payload.selectedDownload.geoname,
        },
        attribute: {
          DataType: 'String',
          StringValue: payload.selectedFieldKey,
        },
        product_key: {
          DataType: 'String',
          StringValue: payload.selectedDownload.product_key,
        },
      },
      MessageBody: JSON.stringify(payload),
      MessageDeduplicationId: uuidv4(),
      MessageGroupId: 'standard',
      QueueUrl: sortQueueUrl,
    };

    sqs.sendMessage(params, function (err, data) {
      if (err) {
        console.error(err);
        return res.status(500).send('Unable to send SQS message.');
      } else {
        console.log(data);
        console.log('Success', data.MessageId);
        return res.json({ success: 'Successfully sent SQS message' });
      }
    });
  });
  //
  app.get('/processSort', async function (req, res) {
    const sortQueueUrl = config.get('SQS.sortQueueUrl');

    const processSort = data => {
      console.log('processing sort todo');
    };

    readMessage(sortQueueUrl, processSort)
      .then(response => {
        return res.json(response);
      })
      .catch(err => {
        return res.status(500).send(err);
      });
  });

  app.get('/processProduct', async function (req, res) {
    const productQueueUrl = config.get('SQS.productQueueUrl');

    const processProduct = data => {
      console.log('processing product todo');
    };

    readMessage(productQueueUrl, processProduct)
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
