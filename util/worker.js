// @ts-check

const fs = require('fs');
const config = require('config');
const present = require('present');
const { fork } = require('child_process');

const { deleteMessage } = require('./sqsOperations');
const { putFileToS3 } = require('./s3Operations');
const { createInstanceLogger, getUniqueLogfileName } = require('./logger');
const { sendAlertMail } = require('./email');
const { directories, directoryIdLength } = require('./constants');
const { generateRef } = require('./crypto');
const {
  initiateProgressHeartbeat,
  unwindStack,
  initiateFreeMemoryQuery,
  initiateDiskSpaceQuery,
} = require('./misc');
const { cleanDirectory } = require('./filesystemUtil');
const { PresignedPost } = require('aws-sdk/clients/s3');

exports.createContext = function (processor) {
  const processorShort = processor.replace('process', '').toLowerCase();
  const directoryId = generateRef({ log: console, process: [] }, directoryIdLength);
  const logfile = getUniqueLogfileName(processorShort);
  const logpath = `${directories.logDir + directoryId}/${logfile}`;
  const log = createInstanceLogger(logpath);
  return {
    log,
    logfile,
    logpath,
    processor,
    directoryId,
    process: [],
    timeStack: [],
    timeBank: {},
    start: present(),
  };
};

exports.runProcess = async function (ctx, queueUrl, messageProcessor, messages) {
  ctx.process.push({ name: 'runProcess', timestamp: present() });

  ctx.log.info('ENV', { env: process.env.NODE_ENV });
  ctx.log.info('task definition', { ecs_task_definition: config.get('ECS.taskDefinition') });

  const deleteParams = {
    QueueUrl: queueUrl,
    ReceiptHandle: messages.Messages[0].ReceiptHandle,
  };

  let errorFlag = false;

  const progressInterval = initiateProgressHeartbeat(ctx, 30);
  const freeMemoryQuery = initiateFreeMemoryQuery(ctx, 60);
  const diskSpaceQuery = initiateDiskSpaceQuery(ctx, 45);

  // fork a process to keep database alive and constantly reset sqs visibility timeout
  const keepAlive = fork('./util/keepAlive.js');
  keepAlive.send({ msg: 'start', data: deleteParams });

  try {
    ctx.log.info('starting message processor');
    await messageProcessor(ctx, messages);
  } catch (err) {
    ctx.log.error('Fatal Error: ', { err: err.message, stack: err.stack });
    errorFlag = true;
  } finally {
    // discontinue updating visibility timeout
    keepAlive.send({ msg: 'end', data: null });
    clearInterval(progressInterval);
    clearInterval(freeMemoryQuery);
    clearInterval(diskSpaceQuery);

    if (!errorFlag) {
      await deleteMessage(ctx, deleteParams);
    }

    ctx.log.info('timebank', { timeBank: ctx.timeBank });

    const timeSummary = Object.keys(ctx.timeBank)
      .map(key => {
        return { fn: key, t: parseInt(ctx.timeBank[key], 10) };
      })
      .sort((a, b) => {
        return b.t - a.t;
      });

    ctx.log.info('totalElapsed', { totalTime: present() - ctx.start });

    ctx.log.info('time-summary', { timeSummary });

    if (!ctx.isDryRun) {
      console.log('\n\nUploading logfile to S3.');
      await putFileToS3(
        { log: console, process: [], timeBank: {}, timeStack: [] },
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
        await sendAlertMail(
          { log: console, process: [], timeBank: {}, timeStack: [] },
          `${ctx.processor} error`,
          fileData,
        );
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

    unwindStack(ctx, 'runProcess');
    console.log('\nAll complete!\n\n');
  }
};
