const AWS = require('aws-sdk');
const nodemailer = require('nodemailer');

const transporter = nodemailer.createTransport({
  SES: new AWS.SES({
    apiVersion: '2010-12-01',
    region: 'us-east-1',
  }),
});

exports.sendAlertMail = function (subject, content) {
  return new Promise((resolve, reject) => {
    transporter.sendMail(
      {
        from: 'alerts@parcel-outlet.com',
        to: 'danieljtrone@gmail.com',
        subject: subject,
        html: `<pre>${content}</pre>`,
      },
      (err, info) => {
        if (err) {
          console.log(err);
          return reject(err);
        } else {
          console.log('Sent Email');
          return resolve();
        }
      },
    );
  });
};
