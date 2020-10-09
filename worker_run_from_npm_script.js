// @ts-check
const config = require('config');
const { readMessages } = require('./util/sqsOperations');
const { processInbox } = require('./processors/processInbox');
const { processSort } = require('./processors/processSort');
const { processProducts } = require('./processors/processProducts');
const { runProcess, createContext } = require('./util/worker');
const { getStatus } = require('./util/misc');
const { cleanEFS } = require('./util/filesystemUtil');

console.log('Environment: ' + process.env.NODE_ENV);

const inboxQueueUrl = config.get('SQS.inboxQueueUrl');
const sortQueueUrl = config.get('SQS.sortQueueUrl');
const productQueueUrl = config.get('SQS.productQueueUrl');

const baseCtx = { log: console, process: [], timeStack: [], timeBank: {} };

let tries = 0;

async function pollQueues() {
  let foundMessages = false;

  const inboxMessages = await readMessages(baseCtx, inboxQueueUrl, 1);
  if (inboxMessages) {
    foundMessages = true;
    const ctx = createContext('processInbox');
    await runProcess(ctx, inboxQueueUrl, processInbox, inboxMessages);
    baseCtx.log.info('Finished handling inbox message.');
  }

  const sortMessages = await readMessages(baseCtx, sortQueueUrl, 1);
  if (sortMessages) {
    foundMessages = true;
    const ctx = createContext('processSort');
    await runProcess(ctx, sortQueueUrl, processSort, sortMessages);
    baseCtx.log.info('Finished handling sort message.');
  }

  const productMessages = await readMessages(baseCtx, productQueueUrl, 1);
  if (productMessages) {
    foundMessages = true;
    const ctx = createContext('processProducts');
    await runProcess(ctx, productQueueUrl, processProducts, productMessages);
    baseCtx.log.info('Finished handling product message.');
  }

  if (!foundMessages) {
    tries++;
    if (tries < 5) {
      baseCtx.log.info('No messages found. Retry ' + tries);
    } else {
      baseCtx.log.info('No messages found. Out of retries.  Exiting ');
    }
  } else {
    tries = 0;
  }

  if (tries < 5) {
    return pollQueues();
  }
}

getStatus(baseCtx)
  .then(status => {
    baseCtx.log.info('Status: ', status);
    return cleanEFS(baseCtx);
  })
  .then(async () => {
    await pollQueues();
  })
  .catch(err => {
    baseCtx.log.error('Unexpected error: ', { error: err.message, stack: err.stack });
  })
  .finally(() => {
    setTimeout(() => {
      process.exit();
    }, 1000);
  });
