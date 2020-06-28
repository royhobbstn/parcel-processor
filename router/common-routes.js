// @ts-check

const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const { acquireConnection } = require('../util/wrappers/wrapQuery');
const { queryHealth } = require('../util/primitives/queries');
const { log } = require('../util/logger');

exports.commonRouter = async app => {
  //
  app.get('/health', function (req, res) {
    return res.json({ status: 'ok' });
  });

  app.get('/databaseHealth', async function (req, res) {
    try {
      await queryHealth();
      return res.json({ status: 'ok' });
    } catch (e) {
      log.info('database health check failed');
      return res.json({ status: 'fail' });
    }
  });

  app.get('/fetchEnv', async function (req, res) {
    await acquireConnection();
    return res.json({ env: process.env.NODE_ENV });
  });
};
