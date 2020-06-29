// @ts-check

const { execSync } = require('child_process');
const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const { acquireConnection } = require('../util/wrapQuery');
const { queryHealth } = require('../util/queries');
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

  app.get('/status', async function (req, res) {
    //
    const applications = ['tippecanoe', 'ogr2ogr', 'aws', 'asdfasdf'];

    const status = {};

    for (let appName of applications) {
      try {
        execSync('command -v ' + appName);
        status[appName] = 'ok';
      } catch (e) {
        status[appName] = 'failed';
      }
    }

    return res.json(status);
  });
};
