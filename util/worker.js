// @ts-check

const fs = require('fs');
const config = require('config');

const { deleteMessage, initiateVisibilityHeartbeat } = require('./sqsOperations');
const { putFileToS3 } = require('./s3Operations');
const { createInstanceLogger, getUniqueLogfileName } = require('./logger');
const { sendAlertMail } = require('./email');
const { directories, directoryIdLength } = require('./constants');
const { generateRef } = require('./crypto');

exports.createContext = function (processor) {
  const processorShort = processor.replace('process', '').toLowerCase();
  const directoryId = generateRef({ log: console }, directoryIdLength);
  const logfile = getUniqueLogfileName(processorShort);
  const logpath = `${directories.logDir + directoryId}/${logfile}`;
  const log = createInstanceLogger(logpath);
  return { log, logfile, logpath, processor, directoryId };
};

exports.runProcess = async function (ctx, queueUrl, messageProcessor, messages) {
  const deleteParams = {
    QueueUrl: queueUrl,
    ReceiptHandle: messages.Messages[0].ReceiptHandle,
  };

  let errorFlag = false;
  const interval = initiateVisibilityHeartbeat(ctx, deleteParams, 60000, 180);

  try {
    ctx.log.info('starting message processor');
    await messageProcessor(ctx, messages);
  } catch (err) {
    ctx.log.error('Fatal Error: ', { err: err.message, stack: err.stack });
    errorFlag = true;
  } finally {
    // discontinue updating visibility timeout
    clearInterval(interval);

    if (!errorFlag) {
      await deleteMessage(ctx, deleteParams);
    }

    if (!ctx.isDryRun) {
      console.log('\n\nUploading logfile to S3.');
      await putFileToS3(
        { log: console },
        config.get('Buckets.logfilesBucket'),
        `${ctx.messageId}-${ctx.type}.log`,
        ctx.logpath,
        'text/plain',
        true,
      );
      // send email to myself if there was an error
      if (errorFlag) {
        const fileData = fs.readFileSync(ctx.logpath);
        await sendAlertMail(`${ctx.processor} error`, fileData);
      }
    } else {
      console.log('This is a DryRun so no email will be sent, and no logfile uploaded to S3.');
    }
    console.log('\nAll complete!\n\n');
  }
};
