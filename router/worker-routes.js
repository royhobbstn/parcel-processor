// @ts-check

const fs = require('fs');
const config = require('config');
const { processInbox } = require('../processors/processInbox');
const { processSort } = require('../processors/processSort');
const { processProducts } = require('../processors/processProducts');
const { processMessage } = require('../util/sqsOperations');
const { putFileToS3 } = require('../util/s3Operations');
const { createInstanceLogger, getUniqueLogfileName } = require('../util/logger');
const { sendAlertMail } = require('../util/email');
const { directories, directoryIdLength } = require('../util/constants');
const { generateRef } = require('../util/crypto');

exports.appRouter = async app => {
  //
  app.get('/processInbox', async function (req, res) {
    const ctx = createContext('processInbox');
    const inboxQueueUrl = config.get('SQS.inboxQueueUrl');
    return runProcess(ctx, res, inboxQueueUrl, processInbox);
  });

  app.get('/processSort', async function (req, res) {
    const ctx = createContext('processSort');
    const sortQueueUrl = config.get('SQS.sortQueueUrl');
    return runProcess(ctx, res, sortQueueUrl, processSort);
  });

  app.get('/processProducts', async function (req, res) {
    const ctx = createContext('processProducts');
    const productQueueUrl = config.get('SQS.productQueueUrl');
    return runProcess(ctx, res, productQueueUrl, processProducts);
  });
};

function createContext(processor) {
  const processorShort = processor.replace('process', '').toLowerCase();
  const directoryId = generateRef({ log: console }, directoryIdLength);
  const logfile = getUniqueLogfileName(processorShort);
  const logpath = `${directories.logDir + directoryId}/${logfile}`;
  const log = createInstanceLogger(logpath);
  return { log, logfile, logpath, processor, directoryId };
}

async function runProcess(ctx, res, queueUrl, messageProcessor) {
  let message;
  let SqsError = false;

  try {
    message = await processMessage(ctx, queueUrl);
    if (message) {
      ctx.log.info(`Queue message has successfully started processing.`);
    }
    res.json({ status: 'OK', message }); // purposefully return before processing
  } catch (err) {
    SqsError = true;
    ctx.log.error('Failed to Receive or Delete Message', { err: err.message, stack: err.stack });
    return res.status(500).send(err.message);
  } finally {
    if (SqsError || !message) {
      // SqsError: error response already sent by HTTP.  nothing to do.
      // !message: 200 response already sent by HTTP.  nothing to do.
      // no logs generated for either scenario since nothing was processed.
      return;
    }
    let errorFlag;
    messageProcessor(ctx, message)
      .then(() => {
        errorFlag = false;
      })
      .catch(err => {
        ctx.log.error('Fatal Error: ', { err: err.message, stack: err.stack });
        errorFlag = true;
      })
      .finally(async () => {
        if (!ctx.isDryRun) {
          console.log('\n\nUploading logfile to S3.');
          await putFileToS3(
            { log: console },
            config.get('Buckets.logfilesBucket'),
            ctx.logfile,
            ctx.logpath,
            'application/json',
            true,
          );
          // send email to myself if there was an error
          if (errorFlag) {
            const fileData = fs.readFileSync(ctx.logpath);
            await sendAlertMail(`${ctx.processor} error`, fileData);
          }
        }
        console.log('\nAll complete!\n\n');
      });
  }
}
