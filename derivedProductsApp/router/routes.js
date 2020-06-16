const { getConnection } = require('../../util/primitives/queries');
const { getSplittableDownloads } = require('../../util/wrappers/wrapQuery');
const { getObject } = require('../../util/primitives/s3Operations');
const { buckets } = require('../../util/constants');

exports.appRouter = async app => {
  //

  app.get('/queryStatFiles', async function (req, res) {
    console.log('fips: ', req.query.fips);

    const connection = await getConnection();

    const rows = await getSplittableDownloads(connection);

    await connection.end();

    return res.json(rows);
  });

  app.get('/proxyStatFile', async function (req, res) {
    //
    const key = decodeURIComponent(req.query.key);

    // modify key to point to -stat.json rather than .ndgeojson

    const modKey = key.replace('.ndgeojson', '-stat.json');

    const data = await getObject(buckets.productsBucket, modKey);

    return res.json(data);
    //
  });

  //
};
