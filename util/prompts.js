const prompt = require('prompt');
const { sourceTypes } = require('./constants');

exports.sourceTypePrompt = function (sourceNameInput) {
  return new Promise((resolve, reject) => {
    if (
      sourceNameInput.includes('http://') ||
      sourceNameInput.includes('https://') ||
      sourceNameInput.includes('ftp://')
    ) {
      console.log('\nDetermined to be a WEBPAGE source.\n');
      return resolve(sourceTypes.WEBPAGE);
    } else if (sourceNameInput.includes('@') && sourceNameInput.includes('.')) {
      // can probably validate better than this
      console.log('\nDetermined to be an EMAIL source.\n');
      return resolve(sourceTypes.EMAIL);
    } else {
      console.log('\nCould not determine source type.  Prompting...\n');
    }

    prompt.start();

    console.log(`\nPlease enter a number:\n 1: webpage\n 2: email address`);

    prompt.get(['sourceType'], function (err, result) {
      if (err) {
        return reject(err);
      }

      if (result.sourceType !== '1' && result.sourceType !== '2') {
        throw new Error('unexpected sourceType input in prompt');
      }

      const sourceType = result.sourceType === '1' ? sourceTypes.WEBPAGE : sourceTypes.EMAIL;

      return resolve(sourceType);
    });
  });
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

exports.execSummaryPrompt = function () {
  return new Promise((resolve, reject) => {
    prompt.start();

    console.log(`\nDoes this look correct? (y/n):`);

    prompt.get(['OKAY'], function (err, result) {
      if (err) {
        return reject(err);
      }
      if (result.OKAY.toLowerCase() === 'y' || result.OKAY.toLowerCase() === 'yes') {
        return resolve('confirmed');
      }
      return reject('CANCELLED by user!');
    });
  });
};

exports.promptGeoIdentifiers = async function () {
  let fipsDetails;
  do {
    fipsDetails = await getGeoIdentifiers();
  } while (!fipsDetails);
  console.log({ fipsDetails });
  return fipsDetails;
};

// returns either an object with identifiers, or false
async function getGeoIdentifiers() {
  // prompt user for SUMLEV, STATEFIPS, and either COUNTYFIPS or PLACEFIPS
  prompt.start();

  let SUMLEV;
  let STATEFIPS;
  let COUNTYFIPS;
  let PLACEFIPS;

  await new Promise((resolve, reject) => {
    prompt.get(['SUMLEV', 'STATEFIPS'], (err, result) => {
      if (err) {
        console.error(err);
        return reject(err);
      }
      SUMLEV = result.SUMLEV;
      STATEFIPS = result.STATEFIPS;
      return resolve();
    });
  });

  if (!SUMLEV || !STATEFIPS) {
    console.log('Forget to enter SUMLEV or STATEFIPS.  Try again.');
    return false;
  }

  // SUMLEV and STATEFIPS required for all products
  if (SUMLEV.length === 3 && STATEFIPS.length === 2) {
    if (SUMLEV === '040') {
      return {
        SUMLEV,
        STATEFIPS,
        COUNTYFIPS: '000',
      };
    } else if (SUMLEV === '050') {
      await new Promise((resolve, reject) => {
        prompt.get(['COUNTYFIPS'], (err, result) => {
          if (err) {
            console.error(err);
            return reject(err);
          }
          COUNTYFIPS = result.COUNTYFIPS;
          return resolve();
        });
      });

      if (COUNTYFIPS && COUNTYFIPS.length === 5 && COUNTYFIPS.slice(0, 2) === STATEFIPS) {
        COUNTYFIPS = COUNTYFIPS.slice(2);
      }
      if (!COUNTYFIPS || COUNTYFIPS.length !== 3) {
        console.error(
          'County fips codes are 3 digits.  (5 digits will be accepted if the first two digits === STATEFIPS.  Try again.',
        );
        return false;
      }
      return {
        SUMLEV,
        STATEFIPS,
        COUNTYFIPS,
      };
    } else if (SUMLEV === '160') {
      await new Promise((resolve, reject) => {
        prompt.get(['PLACEFIPS'], function (err, result) {
          if (err) {
            console.error(err);
            return reject(err);
          }
          PLACEFIPS = result.PLACEFIPS;
          return resolve();
        });
      });

      if (PLACEFIPS && PLACEFIPS.length === 7 && PLACEFIPS.slice(0, 2) === STATEFIPS) {
        PLACEFIPS = PLACEFIPS.slice(2);
      }
      if (!PLACEFIPS || PLACEFIPS.length !== 5) {
        console.error(
          'Place fips codes are 5 digits.  (7 digits will be accepted if the first two digits === STATEFIPS.  Try again.',
        );
        return false;
      }
      return {
        SUMLEV,
        STATEFIPS,
        PLACEFIPS,
      };
    } else {
      console.error(`SUMLEV: Expected either '040' (state), '050' (county), or '160' (place)`);
      return false;
    }
  } else {
    console.error(
      "Invalid input for SUMLEV or STATEFIPS.  SUMLEV is 3 digits and must be either '040' (state), '050' (county), or '160' (place).  STATEFIPS must be 2 digits only.",
    );
    return false;
  }
}

exports.chooseGeoLayer = async function (total_layers) {
  let chosenLayer = 0;

  if (total_layers !== 1) {
    // if just one layer, use it
    // otherwise, prompt for layer number

    const table_choice = await new Promise((resolve, reject) => {
      prompt.start();

      prompt.get(['layer'], function (err, result) {
        if (err) {
          return reject(err);
        }
        console.log('Command-line input received:');
        console.log('  layer: ' + result.layer);
        return resolve(result.layer);
      });
    });
    chosenLayer = parseInt(table_choice);

    console.log({ chosenLayer });
  }

  return chosenLayer;
};
