const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-2' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });

const config = require('config');
const { getTileDatasets, queryProductByIndividualRef } = require('../util/queries');
const { deleteItem } = require('../util/deleteItems');
const ctx = { log: console, process: [], timeBank: {}, timeStack: [] };

const createdBefore = '2020-10-26 22:34:41';
const limit = 1000;

async function main() {
  // require NODE_ENV  to be explicitly set
  const env = process.env.NODE_ENV;
  if (!env) {
    throw new Error('you forgot to set NODE_ENV');
  }

  // get all tile records from database
  const tileDatasets = await getTileDatasets(ctx, createdBefore, limit);

  if (!tileDatasets.length) {
    ctx.log.warn('no eligible datasets left');
    return;
  }

  // for each tile record
  for (let dataset of tileDatasets) {
    const individualRef = dataset.individual_ref;
    // get delete task records
    const tasks = await queryProductByIndividualRef(ctx, individualRef);

    const taskRecords = [...mapToDbDeletes(tasks, true, env), ...mapToS3Deletes(tasks, true, env)];

    // delete each item in task list
    const output = [];
    const sortedItems = taskRecords.sort((a, z) => {
      return a.priority - z.priority;
    });

    try {
      for (let item of sortedItems) {
        const response = await deleteItem(ctx, item);
        output.push(response);
      }
      ctx.log.info('Deletions processed');
      ctx.log.info('output', { output });
    } catch (err) {
      ctx.log.error('Unable to delete item(s)', { error: err.message, stack: err.stack });
    }

    // it's okay to send this even if the above fails.
    const params = {
      MessageAttributes: {},
      MessageBody: dataset.message_body,
      QueueUrl: config.get('SQS.productQueueUrl'),
    };

    await new Promise((resolve, reject) => {
      sqs.sendMessage(params, (err, data) => {
        ctx.log.info('SQS response: ', { data });
        if (err) {
          ctx.log.error(
            `Unable to send SQS message to queue: ${dataset.geoid} ${dataset.product_key}`,
            {
              err: err.message,
              stack: err.stack,
            },
          );
          return reject(err);
        } else {
          ctx.log.info(
            `Successfully sent SQS message to queue: ${dataset.geoid} ${dataset.product_key}`,
          );
          return resolve();
        }
      });
    });
  }
}

main();

// I'd import these but they are not commonJS

function mapToDbDeletes(arr, filterOutNdGeoJson, env) {
  const deletes = [];

  arr
    .filter(d => {
      if (filterOutNdGeoJson) {
        return d.product_type !== 'ndgeojson';
      }
      return true;
    })
    .forEach(item => {
      deletes.push({
        task_name: 'product_row',
        table_name: 'products',
        record_id: item.product_id,
        product_type: item.product_type,
        bucket_name: '',
        bucket_key: '',
        geoid: item.geoid,
        env,
        priority: 3,
      });
    });

  return deletes;
}

function mapToS3Deletes(arr, filterOutNdGeoJson, env) {
  const s3deletes = [];

  arr
    .filter(d => {
      if (filterOutNdGeoJson) {
        return d.product_type !== 'ndgeojson';
      }
      return true;
    })
    .forEach(item => {
      s3deletes.push({
        task_name: item.product_type === 'pbf' ? 'tile_directory' : 'product_file',
        table_name: '',
        record_id: '',
        product_type: item.product_type,
        bucket_name: item.product_type === 'pbf' ? 'tile-server-po' : 'data-products-po',
        bucket_key: item.product_key,
        geoid: item.geoid,
        env,
        priority: 1,
      });

      if (item.product_type === 'ndgeojson') {
        // stat file
        s3deletes.push({
          task_name: 'stat_file',
          table_name: '',
          record_id: '',
          product_type: item.product_type,
          bucket_name: 'data-products-po',
          bucket_key: item.product_key,
          geoid: item.geoid,
          env,
          priority: 1,
        });
      }
    });

  return s3deletes;
}
