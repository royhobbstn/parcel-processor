// @ts-check

const { createLogger, format, transports } = require('winston');
const { combine, timestamp } = format;

const log = createLogger({
  level: 'debug',
  format: combine(timestamp(), format.json()),
  transports: [
    new transports.Console({
      format: combine(format.colorize(), format.simple()),
    }),
  ],
});

exports.log = log;
