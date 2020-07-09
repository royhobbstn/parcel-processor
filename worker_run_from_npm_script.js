// @ts-check
const config = require('config');
const { readMessages } = require('./util/sqsOperations');
const { processInbox } = require('./processors/processInbox');
const { processSort } = require('./processors/processSort');
const { processProducts } = require('./processors/processProducts');
const { runProcess, createContext } = require('./util/worker');
const { getStatus, initiateFreeMemoryQuery } = require('./util/misc');

console.log('Environment: ' + process.env.NODE_ENV);

const inboxQueueUrl = config.get('SQS.inboxQueueUrl');
const sortQueueUrl = config.get('SQS.sortQueueUrl');
const productQueueUrl = config.get('SQS.productQueueUrl');

const baseCtx = { log: console };

initiateFreeMemoryQuery(baseCtx);

async function pollQueues() {
  const inboxMessages = await readMessages(baseCtx, inboxQueueUrl, 1);
  if (inboxMessages) {
    const ctx = createContext('processInbox');
    await runProcess(ctx, inboxQueueUrl, processInbox, inboxMessages);
    baseCtx.log.info('Finished handling inbox message.');
  }

  const sortMessages = await readMessages(baseCtx, sortQueueUrl, 1);
  if (sortMessages) {
    const ctx = createContext('processSort');
    await runProcess(ctx, sortQueueUrl, processSort, sortMessages);
    baseCtx.log.info('Finished handling sort message.');
  }

  const productMessages = await readMessages(baseCtx, productQueueUrl, 1);
  if (productMessages) {
    const ctx = createContext('processProducts');
    await runProcess(ctx, productQueueUrl, processProducts, productMessages);
    baseCtx.log.info('Finished handling product message.');
  }
}

getStatus(baseCtx).then(status => {
  baseCtx.log.info('Status: ', status);
  pollQueues();
});
