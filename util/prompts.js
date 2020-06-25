// @ts-check

const prompt = require('prompt');
const { sourceTypes, modes } = require('./constants');
const { log } = require('./logger');

exports.getSourceType = function (sourceNameInput) {
  if (
    sourceNameInput.includes('http://') ||
    sourceNameInput.includes('https://') ||
    sourceNameInput.includes('ftp://')
  ) {
    log.info('\nDetermined to be a WEBPAGE source.\n');
    return sourceTypes.WEBPAGE;
  } else if (sourceNameInput.includes('@') && sourceNameInput.includes('.')) {
    // can probably validate better than this
    log.info('\nDetermined to be an EMAIL source.\n');
    return sourceTypes.EMAIL;
  } else {
    throw new Error('Could not determine source type.');
  }
};

exports.sourceInputPrompt = function () {
  return new Promise((resolve, reject) => {
    prompt.start();

    console.log(`\nPlease enter the webpage or email address of the source:`);

    prompt.get(['sourceName'], function (err, result) {
      if (err) {
        return reject(err);
      }
      return resolve(result.sourceName);
    });
  });
};

exports.promptGeoIdentifiers = async function () {
  let fipsDetails;
  do {
    fipsDetails = await getGeoIdentifiers();
  } while (fipsDetails.hasError);
  log.info({ fipsDetails });
  return fipsDetails;
};

// returns either an object with identifiers, or false
async function getGeoIdentifiers() {
  // prompt user for SUMLEV, STATEFIPS, and either COUNTYFIPS or PLACEFIPS
  prompt.start();

  const id = {
    SUMLEV: '',
    STATEFIPS: '',
    COUNTYFIPS: '',
    PLACEFIPS: '',
    hasError: false,
  };

  await new Promise((resolve, reject) => {
    prompt.get(['SUMLEV', 'STATEFIPS'], (err, result) => {
      if (err) {
        log.error(err);
        return reject(err);
      }
      id.SUMLEV = result.SUMLEV;
      id.STATEFIPS = result.STATEFIPS;
      return resolve();
    });
  });

  if (!id.SUMLEV || !id.STATEFIPS) {
    console.log('\nForgot to enter SUMLEV or STATEFIPS.  Try again.');
    id.hasError = true;
    return id;
  }

  // SUMLEV and STATEFIPS required for all products
  if (id.SUMLEV.length === 3 && id.STATEFIPS.length === 2) {
    if (id.SUMLEV === '040') {
      id.COUNTYFIPS = '000';
      return id;
    } else if (id.SUMLEV === '050') {
      await new Promise((resolve, reject) => {
        prompt.get(['COUNTYFIPS'], (err, result) => {
          if (err) {
            log.error(err);
            return reject(err);
          }
          id.COUNTYFIPS = result.COUNTYFIPS;
          return resolve();
        });
      });

      if (
        id.COUNTYFIPS &&
        id.COUNTYFIPS.length === 5 &&
        id.COUNTYFIPS.slice(0, 2) === id.STATEFIPS
      ) {
        id.COUNTYFIPS = id.COUNTYFIPS.slice(2);
      }
      if (!id.COUNTYFIPS || id.COUNTYFIPS.length !== 3) {
        console.log(
          '\nCounty fips codes are 3 digits.  (5 digits will be accepted if the first two digits === STATEFIPS.  Try again.',
        );
        id.hasError = true;
        return id;
      }
      return id;
    } else if (id.SUMLEV === '160') {
      await new Promise((resolve, reject) => {
        prompt.get(['PLACEFIPS'], function (err, result) {
          if (err) {
            log.error(err);
            return reject(err);
          }
          id.PLACEFIPS = result.PLACEFIPS;
          return resolve();
        });
      });

      if (id.PLACEFIPS && id.PLACEFIPS.length === 7 && id.PLACEFIPS.slice(0, 2) === id.STATEFIPS) {
        id.PLACEFIPS = id.PLACEFIPS.slice(2);
      }
      if (!id.PLACEFIPS || id.PLACEFIPS.length !== 5) {
        console.log(
          '\nPlace fips codes are 5 digits.  (7 digits will be accepted if the first two digits === STATEFIPS.  Try again.',
        );
        id.hasError = true;
        return id;
      }
      return id;
    } else {
      console.log(`\nSUMLEV: Expected either '040' (state), '050' (county), or '160' (place)`);
      id.hasError = true;
      return id;
    }
  } else {
    console.log(
      "\nInvalid input for SUMLEV or STATEFIPS.  SUMLEV is 3 digits and must be either '040' (state), '050' (county), or '160' (place).  STATEFIPS must be 2 digits only.",
    );
    id.hasError = true;
    return id;
  }
}

exports.modePrompt = function () {
  return new Promise((resolve, reject) => {
    prompt.start();

    console.log(
      `\nType 'full' for a full run.  \nAny other response is a dry-run with no uploads to s3 and no database writes.\n`,
    );

    prompt.get(['mode'], function (err, result) {
      if (err) {
        return reject(err);
      }

      let mode;

      if (result.mode === modes.FULL_RUN.input) {
        mode = modes.FULL_RUN;
      } else {
        mode = modes.DRY_RUN;
      }
      log.info(`Running in ${process.env.NODE_ENV} as ${mode.label} mode.`);
      return resolve(mode);
    });
  });
};
