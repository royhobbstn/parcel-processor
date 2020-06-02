const prompt = require('prompt');

exports.pageInputPrompt = function () {
  return new Promise((resolve, reject) => {
    prompt.start();

    prompt.get(['pageName'], function (err, result) {
      if (err) {
        reject(err);
      }
      console.log('Command-line input received:');
      console.log('  layer: ' + result.pageName);
      resolve(result.pageName);
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
        reject(err);
      }
      SUMLEV = result.SUMLEV;
      STATEFIPS = result.STATEFIPS;
      resolve();
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
            reject(err);
          }
          COUNTYFIPS = result.COUNTYFIPS;
          resolve();
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
            reject(err);
          }
          PLACEFIPS = result.PLACEFIPS;
          resolve();
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
          reject(err);
        }
        console.log('Command-line input received:');
        console.log('  layer: ' + result.layer);
        resolve(result.layer);
      });
    });
    chosenLayer = parseInt(table_choice);

    console.log({ chosenLayer });
  }

  return chosenLayer;
};
