// @ts-check

const AWS = require('aws-sdk');
const nodemailer = require('nodemailer');
const config = require('config');
const { processInbox } = require('../processors/processInbox');
const { processSort } = require('../processors/processSort');
const { processProducts } = require('../processors/processProducts');
const { processMessage } = require('../util/sqsOperations');
const { createInstanceLogger, getUniqueLogfileName } = require('../util/logger');

const transporter = nodemailer.createTransport({
  SES: new AWS.SES({
    apiVersion: '2010-12-01',
    region: 'us-east-1',
  }),
});

// send some mail
transporter.sendMail(
  {
    from: 'danieljtrone@gmail.com',
    to: 'danieljtrone@gmail.com',
    subject: 'Error in Sort 2',
    html: '<b>HTML EMAIL BODY</b><pre>var js = 5;</pre>',
  },
  (err, info) => {
    if (err) {
      console.log(err);
    } else {
      console.log(info);
    }
  },
);

exports.appRouter = async app => {
  //
  app.get('/processInbox', async function (req, res) {
    const logfile = getUniqueLogfileName('inbox');
    const log = createInstanceLogger(logfile);
    const ctx = { log };

    const inboxQueueUrl = config.get('SQS.inboxQueueUrl');
    let message;

    try {
      message = await processMessage(ctx, inboxQueueUrl);
      if (message) {
        ctx.log.info(`Inbox Queue message has successfully started processing.`);
      }
      res.json({ status: 'OK', message }); // purposefully return before processing
    } catch (err) {
      ctx.log.error('Failed to Receive or Delete Message', { err: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    } finally {
      if (!message) return;
      let errorFlag;
      processInbox(message)
        .then(() => {
          errorFlag = false;
        })
        .catch(err => {
          ctx.log.error('Fatal Error: ', { err: err.message, stack: err.stack });
          errorFlag = true;
        })
        .finally(() => {
          // upload Logfile to S3
          // save to DB
          if (errorFlag) {
            // email S3 logfile link to ME - research formatting body of email for JSON
          }
        });
    }
  });

  app.get('/processSort', async function (req, res) {
    const logfile = getUniqueLogfileName('sort');
    const log = createInstanceLogger(logfile);
    const ctx = { log };

    const sortQueueUrl = config.get('SQS.sortQueueUrl');
    let message;

    try {
      message = await processMessage(ctx, sortQueueUrl);
      if (message) {
        ctx.log.info(`Sort Queue message has successfully started processing.`);
      }
      res.json({ status: 'OK', message }); // purposefully return before processing
    } catch (err) {
      ctx.log.error('Failed to Receive or Delete Message', { err: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    } finally {
      if (!message) return;
      let errorFlag;
      processSort(message)
        .then(() => {
          errorFlag = false;
        })
        .catch(err => {
          ctx.log.error('Fatal Error: ', { err: err.message, stack: err.stack });
          errorFlag = true;
        })
        .finally(() => {
          // upload Logfile to S3
          // save to DB
          if (errorFlag) {
            // email S3 logfile link to ME - research formatting body of email for JSON
          }
        });
    }
  });

  app.get('/processProducts', async function (req, res) {
    const logfile = getUniqueLogfileName('product');
    const log = createInstanceLogger(logfile);
    const ctx = { log };

    const productQueueUrl = config.get('SQS.productQueueUrl');
    let message;

    try {
      message = await processMessage(ctx, productQueueUrl);
      if (message) {
        ctx.log.info(`Product Queue message has successfully started processing.`);
      }
      res.json({ status: 'OK', message }); // purposefully return before processing
    } catch (err) {
      ctx.log.error('Failed to Receive or Delete Message', { err: err.message, stack: err.stack });
      return res.status(500).send(err.message);
    } finally {
      if (!message) return;
      let errorFlag;
      processProducts(message)
        .then(() => {
          errorFlag = false;
        })
        .catch(err => {
          ctx.log.error('Fatal Error: ', { err: err.message, stack: err.stack });
          errorFlag = true;
        })
        .finally(() => {
          // upload Logfile to S3
          // save to DB
          if (errorFlag) {
            // email S3 logfile link to ME - research formatting body of email for JSON
          }
        });
    }
  });
};
