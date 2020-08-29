// @ts-check

const fs = require('fs');
const config = require('config');

const { deleteMessage, initiateVisibilityHeartbeat } = require('./sqsOperations');
const { putFileToS3 } = require('./s3Operations');
const { createInstanceLogger, getUniqueLogfileName } = require('./logger');
const { sendAlertMail } = require('./email');
const { directories, directoryIdLength } = require('./constants');
const { generateRef } = require('./crypto');
const { initiateDatabaseHeartbeat } = require('./wrapQuery');
const { initiateProgressHeartbeat, unwindStack } = require('./misc');
const { cleanDirectory } = require('./filesystemUtil');

exports.createContext = function (processor) {
  const processorShort = processor.replace('process', '').toLowerCase();
  const directoryId = generateRef({ log: console, process: [] }, directoryIdLength);
  const logfile = getUniqueLogfileName(processorShort);
  const logpath = `${directories.logDir + directoryId}/${logfile}`;
  const log = createInstanceLogger(logpath);
  return { log, logfile, logpath, processor, directoryId, process: [] };
};

exports.runProcess = async function (ctx, queueUrl, messageProcessor, messages) {
  ctx.process.push('runProcess');

  ctx.log.info('ENV', { env: process.env.NODE_ENV });
  ctx.log.info('task definition', { ecs_task_definition: config.get('ECS.taskDefinition') });

  const deleteParams = {
    QueueUrl: queueUrl,
    ReceiptHandle: messages.Messages[0].ReceiptHandle,
  };

  let errorFlag = false;
  const interval = initiateVisibilityHeartbeat(ctx, deleteParams, 60000, 180);
  const databaseInterval = initiateDatabaseHeartbeat(ctx, 180);
  const progressInterval = initiateProgressHeartbeat(ctx, 30);

  try {
    ctx.log.info('starting message processor');
    await messageProcessor(ctx, messages);
  } catch (err) {
    ctx.log.error('Fatal Error: ', { err: err.message, stack: err.stack });
    errorFlag = true;
  } finally {
    // discontinue updating visibility timeout
    clearInterval(interval);
    clearInterval(databaseInterval);
    clearInterval(progressInterval);

    if (!errorFlag) {
      await deleteMessage(ctx, deleteParams);
    }

    if (!ctx.isDryRun) {
      console.log('\n\nUploading logfile to S3.');
      await putFileToS3(
        { log: console, process: [] },
        config.get('Buckets.logfilesBucket'),
        `${ctx.messageId}-${ctx.type}.log`,
        ctx.logpath,
        'text/plain',
        true,
        false,
      );
      // send email to myself if there was an error
      if (errorFlag) {
        const fileData = fs.readFileSync(ctx.logpath);
        await sendAlertMail({ log: console, process: [] }, `${ctx.processor} error`, fileData);
      }
    } else {
      console.log('This is a DryRun so no email will be sent, and no logfile uploaded to S3.');
    }

    await cleanDirectory(ctx, `${directories.outputDir + ctx.directoryId}`);
    await cleanDirectory(ctx, `${directories.rawDir + ctx.directoryId}`);
    await cleanDirectory(ctx, `${directories.unzippedDir + ctx.directoryId}`);
    // await cleanDirectory(ctx, `${directories.productTempDir + ctx.directoryId}`);
    await cleanDirectory(ctx, `${directories.logDir + ctx.directoryId}`);
    // await cleanDirectory(ctx, `${directories.tilesDir + ctx.directoryId}`);
    await cleanDirectory(ctx, `${directories.subGeographiesDir + ctx.directoryId}`);

    unwindStack(ctx.process, 'runProcess');
    console.log('\nAll complete!\n\n');
  }
};
