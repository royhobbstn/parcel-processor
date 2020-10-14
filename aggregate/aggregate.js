// @ts-check

const fs = require('fs');
const path = require('path');
const geojsonRbush = require('geojson-rbush').default;
const { computeFeature } = require('./computeFeature.js');
const turf = require('@turf/turf');
const { idPrefix, zoomLevels } = require('../util/constants');
const ndjson = require('ndjson');
const { unwindStack, getTimestamp } = require('../util/misc');
const TinyQueue = require('tinyqueue');
const mapshaper = require('mapshaper');

const HUGE_THRESHOLD = 94684125;

exports.runAggregate = runAggregate;

async function runAggregate(ctx, clusterFilePath, aggregatedNdgeojsonBase, aggregationLevel = 0) {
  ctx.process.push({ name: 'runAggregate' + aggregationLevel, timestamp: getTimestamp() });

  // @ts-ignore
  let queue = new TinyQueue([], (a, b) => {
    return a.coalescability - b.coalescability;
  });

  const extension = path.extname(clusterFilePath);
  const file = path.basename(clusterFilePath, extension);
  const cluster = file.split('_')[1];
  ctx.log.info(`aggregating cluster: ${cluster}`);

  /*** Mutable Globals ***/

  const keyed_geojson = {};
  const threshold = [];
  let geojson_feature_count = 0;

  /*** Initial index creation and calculation ***/

  const tree = geojsonRbush();

  const collection = turf.featureCollection([]);

  await new Promise((resolve, reject) => {
    fs.createReadStream(`${clusterFilePath}`)
      .pipe(ndjson.parse({ strict: false }))
      .on('data', async function (obj) {
        // make a new feature with only __po_id
        obj.properties = { [idPrefix]: obj.properties[idPrefix] };

        collection.features.push(obj);
      })
      .on('error', err => {
        ctx.log.warn('Error', { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('end', async () => {
        ctx.log.info(geojson_feature_count + ' records read and indexed');
        return resolve();
      });
  });

  // todo use mapshaper to snap and correct imperfections in dataset.

  const input = { 'input.geojson': JSON.stringify(collection) };
  const cmd = '-i input.geojson snap -clean -o output.geojson';

  // using Promise
  const output = await mapshaper.applyCommands(cmd, input);
  const cleaned = JSON.parse(output['output.geojson'].toString()).features;
  ctx.log.info(`cleaned ${cleaned.length} features`);
  // todo move current processing from in initial load to here

  cleaned.forEach(obj => {
    // only "not huge" features get indexed
    // Think Summit County's Master Parcel
    // (all encompasing parcel that makes up all 'unused' land in County)
    // these parcels can be very complex with hundreds of holes
    // javascript processing cant handle them without locking for sometimes hours
    if (turf.area(obj) < HUGE_THRESHOLD) {
      tree.insert(obj);
    } else {
      ctx.log.warn('Master parcel excluded from index.');
    }

    if (geojson_feature_count % 5000 === 0) {
      ctx.log.info(`${geojson_feature_count} features indexed.`);
    }

    keyed_geojson[obj.properties[idPrefix]] = obj;

    geojson_feature_count++;
  });
  ctx.log.info(`${cleaned.length} features indexed.`);

  // initially seed queue
  for (let [index, key] of Object.keys(keyed_geojson).entries()) {
    // again, no "huge-ish" features allowed into aggregation process
    // they'll eternally hang around in keyed_geojson and be added into every aggregation level
    if (turf.area(keyed_geojson[key]) < HUGE_THRESHOLD) {
      computeFeature(ctx, keyed_geojson[key], tree, queue);
      if (index % 5000 === 0) {
        ctx.log.info(`${index} features computed.`);
      }
    } else {
      ctx.log.warn('Feature greater than threshold.  Excluded.');
    }
  }
  ctx.log.info(`${Object.keys(keyed_geojson).length} features computed.`);
  ctx.log.info(`finished feature computation.  Ready to aggregate.`);

  /********* main ************/

  // continually combine smaller features.
  let errors = 0;
  const RETAINED = getRetained(aggregationLevel);

  /****** Setup ******/

  const STARTING_GEOJSON_FEATURE_COUNT = geojson_feature_count;

  // set an array of feature thresholds
  for (let i = zoomLevels.LOW; i < zoomLevels.HIGH; i = i + 2) {
    threshold.push({ count: Math.round(geojson_feature_count * RETAINED[i]), zoom: i });
  }

  const DESIRED_NUMBER_FEATURES = Math.round(geojson_feature_count * RETAINED[zoomLevels.LOW]);
  const REDUCTIONS_NEEDED = STARTING_GEOJSON_FEATURE_COUNT - DESIRED_NUMBER_FEATURES;

  ctx.log.info('level stats', {
    STARTING_GEOJSON_FEATURE_COUNT,
    DESIRED_NUMBER_FEATURES,
    REDUCTIONS_NEEDED,
  });
  let can_still_simplify = true;

  /****** Do this is in a loop ******/

  while (geojson_feature_count > DESIRED_NUMBER_FEATURES && can_still_simplify) {
    // a zoom level threshold has been reached.  save that zoomlevel.

    for (const obj of threshold) {
      if (geojson_feature_count === obj.count) {
        // convert keyed geojson back to array
        const geojson_array = Object.keys(keyed_geojson).map(feature => {
          return keyed_geojson[feature];
        });
        ctx.log.info('writing zoomlevel: ' + obj.zoom);

        await new Promise(async (resolve, reject) => {
          const output = fs.createWriteStream(
            `${aggregatedNdgeojsonBase}/${obj.zoom}_${cluster}.json`,
          );
          output.on('error', err => {
            reject(err);
          });
          output.on('finish', () => {
            ctx.log.info(`done writing ${aggregatedNdgeojsonBase}/${obj.zoom}_${cluster}.json`);
            resolve();
          });
          for (let row of geojson_array) {
            const continueWriting = output.write(JSON.stringify(row) + '\n');
            if (!continueWriting) {
              await new Promise(resolve => output.once('drain', resolve));
            }
          }
          output.end();
        });
      }
    }

    if (geojson_feature_count % 100 === 0) {
      const progress =
        ((STARTING_GEOJSON_FEATURE_COUNT - geojson_feature_count) / REDUCTIONS_NEEDED) * 100;
      ctx.log.info(`aggregate progress ${progress.toFixed(2)} %`);
    }

    // lowest found, now grab it
    let nextLowest;
    while (queue.length && !nextLowest) {
      const next = queue.pop();

      if (next.match[0] !== -1) {
        nextLowest = next;
      }
    }

    if (!nextLowest) {
      // exhausted all features eligible for combining
      can_still_simplify = false;
      ctx.log.warn('exhausted eligible features for combining.');
    } else {
      const a_match = nextLowest.match;

      // we only use unique_key.  new unique_key is just old unique_keys concatenated with _
      const properties_a = keyed_geojson[a_match[0]].properties;
      const properties_b = keyed_geojson[a_match[1]].properties;

      const area_a = turf.area(keyed_geojson[a_match[0]]);
      const area_b = turf.area(keyed_geojson[a_match[1]]);

      const prop_a = properties_a[idPrefix];
      const prop_b = properties_b[idPrefix];

      const larger_geoid = area_a > area_b ? properties_a[idPrefix] : properties_b[idPrefix];
      const larger_feature = keyed_geojson[larger_geoid];

      let combined;
      try {
        combined = turf.union(keyed_geojson[a_match[0]], keyed_geojson[a_match[1]]);
      } catch (e) {
        // this is not great
        // either they union, or the smaller is filtered out
        // the smaller one might not be the problem geometry
        ctx.log.warn('error - turf union', { error: e.message, stack: e.stack });
        errors++;
      }

      if (!combined) {
        combined = larger_feature;
      }

      // overwrite properties with geoid of larger feature
      combined.properties = {
        [idPrefix]: larger_geoid,
      };

      // delete old features that were combined
      delete keyed_geojson[a_match[0]];
      delete keyed_geojson[a_match[1]];

      // create new combined feature
      keyed_geojson[larger_geoid] = combined;

      // we lost a feature in this combine
      geojson_feature_count--;

      // go back through all features and flag everything that was affected by the above transformation
      const associatedFeatures = new Set();

      // go through queue and flag items with [-1, -1] that have been affected
      // by previous transformation
      // these will be discarded if encountered
      // might be better to in-future: recalculate feature and updates its spot in queue
      // rather than essentially flagging for deletion and creating new items to be inserted
      // think of strategies to only rebuild occasionally
      queue.data.forEach(item => {
        const geoid_array = item.match;

        if (
          geoid_array[0] === prop_a ||
          geoid_array[0] === prop_b ||
          geoid_array[1] === prop_a ||
          geoid_array[1] === prop_b
        ) {
          associatedFeatures.add(geoid_array[0]);
          associatedFeatures.add(geoid_array[1]);
          item.match = [-1, -1];
        }
      });

      associatedFeatures.delete(prop_a);
      associatedFeatures.delete(prop_b);

      // update index (remove previous)
      const options = tree.search(combined);

      options.features.forEach(option => {
        if (
          option.properties[idPrefix] === properties_a[idPrefix] ||
          option.properties[idPrefix] === properties_b[idPrefix]
        ) {
          tree.remove(option);
        }
      });

      // update index (add new)
      tree.insert(combined);

      // recompute features (new combined + all features linked to previous original features)
      computeFeature(ctx, combined, tree, queue);

      associatedFeatures.forEach(id => {
        computeFeature(ctx, keyed_geojson[id], tree, queue);
      });
    }
  }

  if (can_still_simplify) {
    if (errors > 0) {
      ctx.log.warn('errors', { errors });
    }

    // convert keyed geojson back to array
    const geojson_array = Object.keys(keyed_geojson).map(feature => {
      return keyed_geojson[feature];
    });

    // presumably the lowest zoom level doesn't get reached since the loop terminates just before the count hits the target
    // so it is saved here, at the end of the program.
    await new Promise(async (resolve, reject) => {
      const output = fs.createWriteStream(
        `${aggregatedNdgeojsonBase}/${zoomLevels.LOW}_${cluster}.json`,
      );
      output.on('error', err => {
        reject(err);
      });
      output.on('finish', () => {
        ctx.log.info(`done writing ${aggregatedNdgeojsonBase}/${zoomLevels.LOW}_${cluster}.json`);
        resolve();
      });
      for (let row of geojson_array) {
        const continueWriting = output.write(JSON.stringify(row) + '\n');
        if (!continueWriting) {
          await new Promise(resolve => output.once('drain', resolve));
        }
      }
      output.end();
    });

    unwindStack(ctx, 'runAggregate' + aggregationLevel);
    return;
  }

  if (aggregationLevel >= 14) {
    throw new Error('Unable to aggregate.  Ran out of aggregation targets.');
  }

  // if here, need to re-run with less aggressive aggregation targets
  ctx.log.warn('Aggregate failed.  retrying at less aggressive aggregation level.');

  unwindStack(ctx, 'runAggregate' + aggregationLevel);

  return aggregationLevel + 1;
}

// percent of features that will be retained at each zoom level
function getRetained(index) {
  const aggregationLevels = [
    {
      '4': 0.05,
      '6': 0.1,
      '8': 0.15,
      '10': 0.3,
      '12': 0.8,
      '14': 1,
    },
    {
      '4': 0.1,
      '6': 0.2,
      '8': 0.3,
      '10': 0.5,
      '12': 0.8,
      '14': 1,
    },
    {
      '4': 0.2,
      '6': 0.3,
      '8': 0.4,
      '10': 0.6,
      '12': 0.8,
      '14': 1,
    },
    {
      '4': 0.3,
      '6': 0.4,
      '8': 0.5,
      '10': 0.7,
      '12': 0.9,
      '14': 1,
    },
    {
      '4': 0.4,
      '6': 0.5,
      '8': 0.6,
      '10': 0.8,
      '12': 0.9,
      '14': 1,
    },
    {
      '4': 0.5,
      '6': 0.6,
      '8': 0.7,
      '10': 0.8,
      '12': 0.9,
      '14': 1,
    },
    {
      '4': 0.6,
      '6': 0.65,
      '8': 0.7,
      '10': 0.8,
      '12': 0.9,
      '14': 1,
    },
    {
      '4': 0.65,
      '6': 0.7,
      '8': 0.75,
      '10': 0.8,
      '12': 0.9,
      '14': 1,
    },
    {
      '4': 0.7,
      '6': 0.75,
      '8': 0.8,
      '10': 0.85,
      '12': 0.9,
      '14': 1,
    },
    {
      '4': 0.75,
      '6': 0.8,
      '8': 0.85,
      '10': 0.9,
      '12': 0.95,
      '14': 1,
    },
    {
      '4': 0.8,
      '6': 0.84,
      '8': 0.88,
      '10': 0.92,
      '12': 0.96,
      '14': 1,
    },
    {
      '4': 0.85,
      '6': 0.88,
      '8': 0.91,
      '10': 0.94,
      '12': 0.97,
      '14': 1,
    },
    {
      '4': 0.9,
      '6': 0.92,
      '8': 0.94,
      '10': 0.96,
      '12': 0.98,
      '14': 1,
    },
    {
      '4': 0.95,
      '6': 0.96,
      '8': 0.97,
      '10': 0.98,
      '12': 0.99,
      '14': 1,
    },
  ];

  return aggregationLevels[index];
}
