const { getConnection } = require('../../util/primitives/queries');
const { getSplittableDownloads, getCountiesByState } = require('../../util/wrappers/wrapQuery');
const { getObject } = require('../../util/primitives/s3Operations');
const { buckets } = require('../../util/constants');

exports.appRouter = async app => {
  //

  app.get('/queryStatFiles', async function (req, res) {
    const geoid = req.query.geoid;
    const connection = await getConnection();
    const rows = await getSplittableDownloads(connection, geoid);
    await connection.end();
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
    const connection = await getConnection();
    const rows = await getCountiesByState(connection, geoid);
    await connection.end();
    return res.json(rows);
  });
  //
};
