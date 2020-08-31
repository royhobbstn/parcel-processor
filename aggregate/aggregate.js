// @ts-check

const fs = require('fs');
const path = require('path');
const geojsonRbush = require('geojson-rbush').default;
const { computeFeature } = require('./computeFeature.js');
const present = require('present');
const turf = require('@turf/turf');
const { idPrefix, directories, zoomLevels } = require('../util/constants');
const { promisify } = require('util');
const writeFileAsync = promisify(fs.writeFile);
const ndjson = require('ndjson');
const { unwindStack, sleep } = require('../util/misc');
const TinyQueue = require('tinyqueue');

// @ts-ignore
let queue = new TinyQueue([], (a, b) => {
  return a.coalescability - b.coalescability;
});

exports.runAggregate = async function (ctx, clusterFilePath) {
  ctx.process.push('runAggregate');

  const extension = path.extname(clusterFilePath);
  const file = path.basename(clusterFilePath, extension);
  const cluster = file.split('_')[1];
  ctx.log.info(`aggregating cluster: ${cluster}`);

  /*** Mutable Globals ***/

  const keyed_geojson = {};
  const threshold = [];
  const written_file_promises = [];
  let geojson_feature_count = 0;
  const attributeLookupFile = {};

  /*** Initial index creation and calculation ***/

  const tree = geojsonRbush();

  await new Promise((resolve, reject) => {
    fs.createReadStream(`${clusterFilePath}`)
      .pipe(ndjson.parse())
      .on('data', async function (obj) {
        // save attributes to lookup file
        attributeLookupFile[obj.properties[idPrefix]] = JSON.parse(JSON.stringify(obj.properties));

        // make a new feature with only __po_id
        obj.properties = { [idPrefix]: obj.properties[idPrefix] };

        tree.insert(obj);

        if (geojson_feature_count % 2000 === 0) {
          ctx.log.info(`${geojson_feature_count} features indexed.`);
        }

        keyed_geojson[obj.properties[idPrefix]] = obj;

        geojson_feature_count++;
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('end', async () => {
        ctx.log.info(geojson_feature_count + ' records read and indexed');
        return resolve();
      });
  });

  // initially seed queue
  Object.keys(keyed_geojson).forEach(async (key, index) => {
    computeFeature(ctx, keyed_geojson[key], tree, queue);
    if (index % 2000 === 0) {
      ctx.log.info(`${index} features computed.`);
      await sleep(100);
    }
  });
  ctx.log.info(`finished feature computation.  Ready to aggregate.`);

  /********* main ************/

  // continually combine smaller features.

  const total_time = present(); // tracks total execution time
  let cftime = 0;
  let turfUnion = 0;

  let errors = 0;
  let errorIds = new Set();
  const RETAINED = getRetained();

  /****** Setup ******/

  const STARTING_GEOJSON_FEATURE_COUNT = geojson_feature_count;

  // set an array of feature thresholds
  for (let i = zoomLevels.LOW; i < zoomLevels.HIGH; i = i + 2) {
    threshold.push({ count: Math.round(geojson_feature_count * RETAINED[i]), zoom: i });
  }

  const DESIRED_NUMBER_FEATURES = Math.round(geojson_feature_count * RETAINED[zoomLevels.LOW]);
  const REDUCTIONS_NEEDED = STARTING_GEOJSON_FEATURE_COUNT - DESIRED_NUMBER_FEATURES;

  let can_still_simplify = true;

  /****** Do this is in a loop ******/

  while (geojson_feature_count > DESIRED_NUMBER_FEATURES && can_still_simplify) {
    // a zoom level threshold has been reached.  save that zoomlevel.
    threshold.forEach(obj => {
      if (geojson_feature_count === obj.count) {
        // convert keyed geojson back to array
        const geojson_array = Object.keys(keyed_geojson).map(feature => {
          return keyed_geojson[feature];
        });
        ctx.log.info('writing zoomlevel: ' + obj.zoom);
        written_file_promises.push(
          writeFileAsync(
            `${directories.productTempDir + ctx.directoryId}/aggregated_${
              obj.zoom
            }|${cluster}.json`,
            JSON.stringify(turf.featureCollection(geojson_array)),
            'utf8',
          ),
        );
      }
    });

    if (geojson_feature_count % 500 === 0) {
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
    } else {
      const a_match = nextLowest.match;

      let properties_a, properties_b;
      // we only use unique_key.  new unique_key is just old unique_keys concatenated with _
      try {
        properties_a = keyed_geojson[a_match[0]].properties;
        properties_b = keyed_geojson[a_match[1]].properties;
      } catch (e) {
        console.log(e);
        console.log({ a_match });
        throw e;
      }

      const area_a = turf.area(keyed_geojson[a_match[0]]);
      const area_b = turf.area(keyed_geojson[a_match[1]]);

      const prop_a = properties_a[idPrefix];
      const prop_b = properties_b[idPrefix];

      const larger_geoid = area_a > area_b ? properties_a[idPrefix] : properties_b[idPrefix];
      const larger_feature =
        area_a > area_b ? keyed_geojson[a_match[0]] : keyed_geojson[a_match[1]];

      const tu = present();
      let combined;
      try {
        combined = turf.union(keyed_geojson[a_match[0]], keyed_geojson[a_match[1]]);
      } catch (e) {
        // todo set up test with these two features
        // so that if you preprocess them a certain way
        // either they union, or they are filtered out.
        ctx.log.warn('error', { error: e.message });
        errors++;
        errorIds.add(a_match[0]);
        errorIds.add(a_match[1]);
      }
      turfUnion += present() - tu;

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

      // go back through all features and remove everything that was affected by the above transformation
      // todo shouldnt need to go through EVERYTHING!
      const associatedFeatures = new Set();

      // TODO this messes up the heap.
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
      const cf = present();
      computeFeature(ctx, combined, tree, queue);

      associatedFeatures.forEach(id => {
        computeFeature(ctx, keyed_geojson[id], tree, queue);
      });
      cftime += present() - cf;
    }
  }

  ctx.log.warn('errors', { errors });
  ctx.log.warn('errorIds', { errorIds });

  // convert keyed geojson back to array
  const geojson_array = Object.keys(keyed_geojson).map(feature => {
    return keyed_geojson[feature];
  });

  // presumably the lowest zoom level doesn't get reached since the loop terminates just before the count hits the target
  // so it is saved here, at the end of the program.
  written_file_promises.push(
    writeFileAsync(
      `${directories.productTempDir + ctx.directoryId}/aggregated_${
        zoomLevels.LOW
      }|${cluster}.json`,
      JSON.stringify(turf.featureCollection(geojson_array)),
      'utf8',
    ),
  );

  await Promise.all(written_file_promises);

  ctx.log.info(`Completed aggregation: ${present() - total_time} ms`);
  ctx.log.info(`Compute feature time: ${cftime} ms`);
  ctx.log.info(`Turf Union time: ${turfUnion} ms`);
  unwindStack(ctx.process, 'runAggregate');

  return attributeLookupFile;
};

// percent of features that will be retained at each zoom level
function getRetained() {
  return {
    '4': 0.15,
    '6': 0.3,
    '8': 0.45,
    '10': 0.6,
    '12': 0.8,
    '14': 1,
  };
}
