'use strict';

const AWS = require('aws-sdk');
const nodemailer = require('nodemailer');

const transporter = nodemailer.createTransport({
  SES: new AWS.SES({
    apiVersion: '2010-12-01',
    region: 'us-east-1',
  }),
});

module.exports.hello = async event => {
  const reqBody = JSON.parse(event.body);

  try {
    await new Promise((resolve, reject) => {
      transporter.sendMail(
        {
          from: 'feedback@parcel-outlet.com',
          to: 'danieljtrone@gmail.com',
          subject: 'You have received feedback',
          text: JSON.stringify(reqBody, null, '  '),
          html: `<pre>${JSON.stringify(reqBody, null, '  ')}</pre>`,
        },
        (err, info) => {
          if (err) {
            return reject(err);
          } else {
            console.log('Sent Email');
            return resolve();
          }
        },
      );
    });

    return {
      statusCode: 200,
      body: '{"message": "Email Sent!"}',
    };
  } catch (err) {
    console.log(err);
    return {
      statusCode: 500,
      body: '{"message": "Something went wrong"}',
    };
  }
};
