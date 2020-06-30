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
    const ctx = { log };
    ctx.log.info('Health ok');
    return res.json({ status: 'ok' });
  });

  app.get('/databaseHealth', async function (req, res) {
    const ctx = { log };
    try {
      await queryHealth(ctx);
      ctx.log.info('database health check passed');
      return res.json({ status: 'ok' });
    } catch (e) {
      ctx.log.info('database health check failed');
      return res.json({ status: 'fail' });
    }
  });

  app.get('/fetchEnv', async function (req, res) {
    const ctx = { log };
    try {
      await acquireConnection(ctx);
      return res.json({ env: process.env.NODE_ENV });
    } catch (err) {
      return res.status(500).send(err.message);
    }
  });

  app.get('/status', async function (req, res) {
    const ctx = { log };
    const applications = ['tippecanoe', 'ogr2ogr', 'aws'];
    const status = {};

    for (let appName of applications) {
      try {
        execSync('command -v ' + appName);
        status[appName] = 'ok';
        ctx.log.info(`${appName}: passed`);
      } catch (e) {
        status[appName] = 'failed';
        ctx.log.info(`${appName}: failed`);
      }
    }

    return res.json(status);
  });
};
