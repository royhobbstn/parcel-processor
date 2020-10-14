// @ts-check

const fs = require('fs');
const ss = require('simple-statistics');
const ndjson = require('ndjson');
const { unwindStack, getTimestamp } = require('./misc');
const { sampleSize } = require('lodash');

const SAMPLE = 25000;

exports.parseFieldStatistics = parseFieldStatistics;

async function parseFieldStatistics(ctx, statsFilePath, convertToFormatBase) {
  ctx.process.push({ name: 'parseFieldStatistics', timestamp: getTimestamp() });

  ctx.log.info('statsFilePath', { statsFilePath });
  ctx.log.info('convertToFormatBase', { convertToFormatBase });

  // load stat file
  const statsFile = JSON.parse(fs.readFileSync(statsFilePath, 'utf8'));

  // get list of fields && determine which fields to use
  const fieldsFiltered = Object.keys(statsFile.fields).filter(fieldName => {
    const field = statsFile.fields[fieldName];
    // rule out ID fields
    const isIdField = field.IDField === true;
    // rule out single value fields
    const isSingleValue = Object.keys(field.uniques).length <= 1;
    return !(isIdField || isSingleValue);
  });

  const numericFields = [];
  const categoricalFields = [];

  fieldsFiltered.forEach(fieldName => {
    // rem: these values are okay...ish.  we'll convert them to null
    // as long as all other unique values are numbers, its numeric.
    // console.log(isNumeric(undefined));            // false
    // console.log(isNumeric(''));                   // false
    // console.log(isNumeric(null));                 // false

    // figure out if a field is numerical, categorical, or potentially both
    // partition into numeric and categorical arrays

    const field = statsFile.fields[fieldName];
    if (field.types.includes('number')) {
      // numeric always added to categorical just in case
      numericFields.push(fieldName);
      categoricalFields.push(fieldName);
    } else if (field.types.includes('string')) {
      categoricalFields.push(fieldName);

      // possibility that numeric value is encoded as string.
      // if thats the case, we'll add it to both number and string field arrays
      // (in case the number is not ordinal - like in the case of a categorical number key)
      let foundNumericValues = 0;
      let foundStringValues = 0;
      Object.keys(field.uniques).forEach(uniqueValue => {
        if (uniqueValue === '' || uniqueValue === 'null' || uniqueValue === 'undefined') {
          // these values happen and doesnt rule out numeric
          return;
        }
        if (isNumeric(uniqueValue)) {
          foundNumericValues++;
        } else {
          foundStringValues++;
        }
      });
      // is > 20 because mapping a small number of numeric values doesnt make sense
      // and hints that it might be categorical
      // plus i dont want to deal with problems on trying to divide such a field into breaks
      if (foundNumericValues >= 20 && foundStringValues === 0) {
        numericFields.push(fieldName);
      }
    }
  });

  ctx.log.info('fields found: ', { numericFields, categoricalFields });

  const values = {};

  // loop through ndgeojson.  for each eligible field, keep track of values
  // sample up to 100,000?s
  let geojson_feature_count = 0;

  await new Promise((resolve, reject) => {
    const readStream = fs
      .createReadStream(`${convertToFormatBase}.ndgeojson`)
      .pipe(ndjson.parse({ strict: false }))
      .on('data', async function (obj) {
        readStream.pause();

        // todo we could pipe these each to write streams

        for (let field of numericFields) {
          const num = Number(obj.properties[field]);
          if (!Number.isNaN(num)) {
            if (!values[field]) {
              values[field] = [num];
            } else {
              values[field].push(num);
            }
          }
        }

        if (geojson_feature_count % 10000 === 0) {
          ctx.log.info(`${geojson_feature_count} features parsed for numeric values.`);
        }

        geojson_feature_count++;
        readStream.resume();
      })
      .on('error', err => {
        ctx.log.warn('Error', { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('end', async () => {
        ctx.log.info('done parsing numeric data');
        return resolve();
      });
  });

  const fieldMetadata = { numeric: {}, categorical: {} };

  // numeric fields go through breaks
  for (let fieldName of numericFields) {
    ctx.log.info('Calculating breaks for ' + fieldName);
    const computedBreaks = calcBreaks(ctx, values[fieldName]);
    fieldMetadata.numeric[fieldName] = computedBreaks;
  }

  // categorical fields go through Up to 25 categories
  for (let fieldName of categoricalFields) {
    const uniques = statsFile.fields[fieldName].uniques;
    const arrayUniques = Object.keys(uniques).map(uniqueValue => {
      return { value: uniqueValue, count: uniques[uniqueValue] };
    });
    arrayUniques.sort((a, b) => {
      return b.count - a.count;
    });
    const topUniques = arrayUniques.slice(0, 25).map(d => d.value);
    fieldMetadata.categorical[fieldName] = topUniques;
  }

  unwindStack(ctx, 'parseFieldStatistics');

  return fieldMetadata;
}

function calcBreaks(ctx, data) {
  let thedata = sampleSize(data, SAMPLE);
  ctx.process.push({ name: 'calcBreaks', timestamp: getTimestamp() });

  const max = ss.max(thedata);

  // all values in array are 0. (presumably no bg data)  Add a '1' to the array so simplestatistics library doesnt fail computing ckmeans.
  if (max === 0) {
    ctx.log.warn('max value of field is 0.  filling array with nonsense');
    thedata = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14];
  }

  const min = ss.min(thedata);
  const median = ss.median(thedata);
  const stddev = ss.standardDeviation(thedata);
  const ckmeans5 = ss.ckmeans(thedata, 5);
  const ckmeans7 = ss.ckmeans(thedata, 7);
  const ckmeans9 = ss.ckmeans(thedata, 9);

  const computed_breaks = {};

  try {
    computed_breaks.ckmeans5 = [ckmeans5[1][0], ckmeans5[2][0], ckmeans5[3][0], ckmeans5[4][0]];
  } catch (e) {
    ctx.log.info('thedata', { thedata });
    ctx.log.info('ckmeans5', { ckmeans5 });
    throw e;
  }

  computed_breaks.ckmeans7 = [
    ckmeans7[1][0],
    ckmeans7[2][0],
    ckmeans7[3][0],
    ckmeans7[4][0],
    ckmeans7[5][0],
    ckmeans7[6][0],
  ];
  computed_breaks.ckmeans9 = [
    ckmeans9[1][0],
    ckmeans9[2][0],
    ckmeans9[3][0],
    ckmeans9[4][0],
    ckmeans9[5][0],
    ckmeans9[6][0],
    ckmeans9[7][0],
    ckmeans9[8][0],
  ];
  computed_breaks.stddev7 = [
    median - stddev * 1.5,
    median - stddev,
    median - stddev * 0.5,
    median,
    median + stddev * 0.5,
    median + stddev,
    median + stddev * 1.5,
  ];
  computed_breaks.stddev8 = [
    median - stddev * 2.5,
    median - stddev * 1.5,
    median - stddev * 0.5,
    median + stddev * 0.5,
    median + stddev * 1.5,
    median + stddev * 2.5,
  ];

  computed_breaks.quantile5 = [
    ss.quantile(thedata, 0.2),
    ss.quantile(thedata, 0.4),
    ss.quantile(thedata, 0.6),
    ss.quantile(thedata, 0.8),
  ];

  computed_breaks.quantile7 = [
    ss.quantile(thedata, 0.14286),
    ss.quantile(thedata, 0.28571),
    ss.quantile(thedata, 0.42857),
    ss.quantile(thedata, 0.57143),
    ss.quantile(thedata, 0.714286),
    ss.quantile(thedata, 0.857143),
  ];

  computed_breaks.quantile9 = [
    ss.quantile(thedata, 0.11111),
    ss.quantile(thedata, 0.22222),
    ss.quantile(thedata, 0.33333),
    ss.quantile(thedata, 0.44444),
    ss.quantile(thedata, 0.55555),
    ss.quantile(thedata, 0.66666),
    ss.quantile(thedata, 0.77777),
    ss.quantile(thedata, 0.88888),
  ];

  computed_breaks.quantile11 = [
    ss.quantile(thedata, 0.09091),
    ss.quantile(thedata, 0.18182),
    ss.quantile(thedata, 0.27273),
    ss.quantile(thedata, 0.36364),
    ss.quantile(thedata, 0.45454),
    ss.quantile(thedata, 0.54545),
    ss.quantile(thedata, 0.63636),
    ss.quantile(thedata, 0.72727),
    ss.quantile(thedata, 0.81818),
    ss.quantile(thedata, 0.90909),
  ];

  computed_breaks.min = min;
  computed_breaks.max = max;
  computed_breaks.mean = ss.mean(thedata);
  computed_breaks.median = median;
  computed_breaks.stddev = stddev;

  unwindStack(ctx, 'calcBreaks');

  return computed_breaks;
}

function isNumeric(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}
