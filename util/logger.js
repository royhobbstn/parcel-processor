// @ts-check

const { createLogger, format, transports } = require('winston');
const { logfileNameLength } = require('./constants');
const { generateRef } = require('./crypto');
const { combine, timestamp, ms } = format;

const log = createLogger({
  level: 'debug',
  format: combine(timestamp(), ms(), format.json()),
  transports: [
    new transports.Console({
      format: combine(format.colorize(), format.simple()),
    }),
  ],
});

const getUniqueLogfileName = function (serviceName) {
  const ts = Math.round(new Date().getTime() / 1000);
  const entropy = generateRef({ log: console, process: [] }, logfileNameLength);
  return `${ts}-${serviceName}-${entropy}.log`;
};

const createInstanceLogger = function (fileNameAndPath) {
  const log = createLogger({
    level: 'debug',
    format: combine(timestamp(), ms(), format.json()),
    transports: [
      new transports.Console({
        format: combine(format.colorize(), format.simple()),
        handleExceptions: true,
      }),
      new transports.File({ filename: fileNameAndPath, handleExceptions: true }),
    ],
  });
  return log;
};

exports.log = log;

exports.createInstanceLogger = createInstanceLogger;
exports.getUniqueLogfileName = getUniqueLogfileName;
