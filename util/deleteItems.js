// @ts-check

const { unwindStack, getTimestamp } = require('./misc');
const { removeS3Files } = require('./wrapS3');
const { s3deleteType } = require('./constants');
const { deleteRecordById } = require('./queries');

exports.deleteItem = deleteItem;

async function deleteItem(ctx, item) {
  ctx.process.push({ name: 'deleteItem', timestamp: getTimestamp() });

  let output;
  switch (item.task_name) {
    case 'product_file':
      ctx.log.info(`Deleting product file`, {
        product_type: item.product_type,
        bucket_name: item.bucket_name,
        bucket_key: item.bucket_key,
      });
      try {
        output = await deleteFile(ctx, item, s3deleteType.FILE);
      } catch (err) {
        ctx.log.info(`Delete product file failed: ${err.message}`, {
          error: err.message,
          stack: err.stack,
        });
        output = `Delete product file failed: ${err.message}`;
      }
      break;
    case 'stat_file':
      ctx.log.info(`Deleting stat file`, {
        product_type: item.product_type,
        bucket_name: item.bucket_name,
        bucket_key: item.bucket_key,
      });
      try {
        output = await deleteFileStat(ctx, item);
      } catch (err) {
        ctx.log.info(`Delete stat file failed: ${err.message}`, {
          error: err.message,
          stack: err.stack,
        });
        output = `Delete stat file failed: ${err.message}`;
      }
      break;
    case 'tile_directory':
      ctx.log.info(`Deleting tile directory`, {
        product_type: item.product_type,
        bucket_name: item.bucket_name,
        bucket_key: item.bucket_key,
      });
      try {
        output = await deleteFile(ctx, item, s3deleteType.DIRECTORY);
      } catch (err) {
        ctx.log.info(`Delete tile directory failed: ${err.message}`, {
          error: err.message,
          stack: err.stack,
        });
        output = `Delete tile directory failed: ${err.message}`;
      }
      break;
    case 'raw_file':
      ctx.log.info(`Deleting raw file`, {
        product_type: item.product_type,
        bucket_name: item.bucket_name,
        bucket_key: item.bucket_key,
      });
      try {
        output = await deleteFile(ctx, item, s3deleteType.FILE);
      } catch (err) {
        ctx.log.info(`Delete raw file failed: ${err.message}`, {
          error: err.message,
          stack: err.stack,
        });
        output = `Delete raw file failed: ${err.message}`;
      }
      break;
    case 'product_row':
      ctx.log.info(`Deleting product row`, {
        product_type: item.product_type,
        table_name: item.table_name,
        record_id: item.record_id,
        meta: item.meta,
        geoid: item.geoid,
      });
      try {
        output = await deleteRecordById(ctx, item.table_name, 'product_id', item.record_id);
      } catch (err) {
        ctx.log.info(`Delete product record failed: ${err.message}`, {
          error: err.message,
          stack: err.stack,
        });
        output = `Delete product record failed: ${err.message}`;
      }
      break;
    case 'source-check_row':
      ctx.log.info(`Deleting source-check row`, {
        product_type: item.product_type,
        table_name: item.table_name,
        record_id: item.record_id,
        meta: item.meta,
        geoid: item.geoid,
      });
      try {
        output = await deleteRecordById(ctx, item.table_name, 'check_id', item.record_id);
      } catch (err) {
        ctx.log.info(`Delete source-check record failed: ${err.message}`, {
          error: err.message,
          stack: err.stack,
        });
        output = `Delete source-check record failed: ${err.message}`;
      }
      break;
    case 'download_row':
      ctx.log.info(`Deleting download row`, {
        product_type: item.product_type,
        table_name: item.table_name,
        record_id: item.record_id,
        meta: item.meta,
        geoid: item.geoid,
      });
      try {
        output = await deleteRecordById(ctx, item.table_name, 'download_id', item.record_id);
      } catch (err) {
        ctx.log.info(`Delete download record failed: ${err.message}`, {
          error: err.message,
          stack: err.stack,
        });
        output = `Delete download record failed: ${err.message}`;
      }
      break;
    default:
      throw new Error('unexpected delete item task name');
  }

  unwindStack(ctx, 'deleteItem');
  return output;
}

async function deleteFile(ctx, item, type) {
  ctx.process.push({ name: 'deleteFile', timestamp: getTimestamp() });

  const bucketName =
    item.bucket_name + (item.env === 'development' || item.env === 'test' ? '-dev' : '');
  const output = await removeS3Files(ctx, [{ type, bucket: bucketName, key: item.bucket_key }]);

  unwindStack(ctx, 'deleteFile');
  return output;
}

async function deleteFileStat(ctx, item) {
  ctx.process.push({ name: 'deleteFileStat', timestamp: getTimestamp() });

  const bucketName =
    item.bucket_name + (item.env === 'development' || item.env === 'test' ? '-dev' : '');

  const updatedKey = item.bucket_key.replace('.ndgeojson', '-stat.json');

  const output = await removeS3Files(ctx, [
    { type: s3deleteType.FILE, bucket: bucketName, key: updatedKey },
  ]);

  unwindStack(ctx, 'deleteFileStat');
  return output;
}
