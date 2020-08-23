// @ts-check

const fs = require('fs');
const geojsonRbush = require('geojson-rbush').default;
const { computeFeature } = require('./computeFeature.js');
const present = require('present');
const turf = require('@turf/turf');
const { idPrefix, directories, zoomLevels } = require('../util/constants');
const { promisify } = require('util');
const writeFileAsync = promisify(fs.writeFile);
const ndjson = require('ndjson');
const { unwindStack } = require('../util/misc');

// const dirName = `${downloadRef}-${productRef}-${productRefTiles}`;
// const tilesDir = `${directories.tilesDir + ctx.directoryId}/${dirName}`;

// const derivativePath = `${directories.productTempDir + ctx.directoryId}/${fileNameBase}-cluster`;
// const writer = fs.createWriteStream(`${derivativePath}.ndgeojson`);

function setPause(timer) {
  return new Promise(resolve => {
    setTimeout(() => {
      return resolve();
    }, timer);
  });
}

exports.runAggregate = async function (ctx, derivativePath) {
  ctx.process.push('runAggregate');

  /*** Mutable Globals ***/

  let ordered_arr = [];
  const keyed_geojson = {};
  const threshold = [];
  const written_file_promises = [];
  let geojson_feature_count = 0;
  const attributeLookupFile = {};

  /*** Initial index creation and calculation ***/

  const tree = geojsonRbush();

  await new Promise((resolve, reject) => {
    fs.createReadStream(`${derivativePath}.ndgeojson`)
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

  // initially seed ordered_arr
  Object.keys(keyed_geojson).forEach(async (key, index) => {
    computeFeature(keyed_geojson[key], tree, ordered_arr);
    if (index % 2000 === 0) {
      ctx.log.info(`${index} features computed.`);
      await setPause(100);
    }
  });
  ctx.log.info(`finished feature computation.  Ready to aggregate.`);

  /********* main ************/

  // continually combine smaller features.
  // without aggregating across state or county lines
  // note: this is beautiful

  const total_time = present(); // tracks total execution time

  const RETAINED = getRetained();

  /****** Setup ******/

  const STARTING_GEOJSON_FEATURE_COUNT = geojson_feature_count;

  // set an array of feature thresholds
  for (let i = zoomLevels.LOW; i < zoomLevels.HIGH; i++) {
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
            `${directories.productTempDir + ctx.directoryId}/aggregated_${obj.zoom}.json`,
            JSON.stringify(turf.featureCollection(geojson_array)),
            'utf8',
          ),
        );
      }
    });

    if (geojson_feature_count % 500 === 0) {
      const progress =
        ((STARTING_GEOJSON_FEATURE_COUNT - geojson_feature_count) / REDUCTIONS_NEEDED) * 100;
      ctx.log.info(`compute progress ${progress.toFixed(2)} %`);
    }

    // error check this for nothing left in coalesced_scores array
    let a_match;

    let lowest = Infinity;

    const item = ordered_arr[0];
    const value = item.coalescability;

    if (value < lowest) {
      lowest = value;
    }

    if (lowest === Infinity) {
      // exhausted all features eligible for combining
      a_match = false;
    } else {
      // lowest found, now grab it
      // TODO performance concern with SHIFT!
      const a_next_lowest = ordered_arr.shift();
      a_match = a_next_lowest.match;
    }

    // are there still a pool of features remaining that can be simplified?
    // sometimes constraints such as making sure features are not combined
    // across county lines creates situations where we exhaust the pool of
    // features able to be combined for low (zoomed out) zoom levels
    if (!a_match) {
      can_still_simplify = false;
    } else {
      // we only use unique_key.  new unique_key is just old unique_keys concatenated with _
      let properties_a;
      let properties_b;

      properties_a = keyed_geojson[a_match[0]].properties;
      properties_b = keyed_geojson[a_match[1]].properties;

      const area_a = turf.area(keyed_geojson[a_match[0]]);
      const area_b = turf.area(keyed_geojson[a_match[1]]);
      const prop_a = properties_a[idPrefix];
      const prop_b = properties_b[idPrefix];

      const larger_geoid = area_a > area_b ? properties_a[idPrefix] : properties_b[idPrefix];

      const combined = turf.union(keyed_geojson[a_match[0]], keyed_geojson[a_match[1]]);

      // overwrite properties with geoid of larger feature
      // AA property is a flag for aggregated area
      combined.properties = {
        [idPrefix]: larger_geoid,
      };

      // delete old features that were combined
      delete keyed_geojson[a_match[0]];
      delete keyed_geojson[a_match[1]];

      // create new combined feature
      keyed_geojson[larger_geoid] = combined;

      geojson_feature_count--;

      // go back through all features and remove everything that was affected by the above transformation
      ordered_arr = ordered_arr.filter(item => {
        const geoid_array = item.match;

        if (
          geoid_array[0] === prop_a ||
          geoid_array[0] === prop_b ||
          geoid_array[1] === prop_a ||
          geoid_array[1] === prop_b
        ) {
          return false;
        }
        return true;
      });

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

      // recompute features
      computeFeature(combined, tree, ordered_arr);
    }
  }

  // convert keyed geojson back to array
  const geojson_array = Object.keys(keyed_geojson).map(feature => {
    return keyed_geojson[feature];
  });

  // presumably the lowest zoom level doesn't get reached since the loop terminates just before the count hits the target
  // so it is saved here, at the end of the program.
  written_file_promises.push(
    writeFileAsync(
      `${directories.productTempDir + ctx.directoryId}/aggregated_${zoomLevels.LOW}.json`,
      JSON.stringify(turf.featureCollection(geojson_array)),
      'utf8',
    ),
  );

  await Promise.all(written_file_promises);

  ctx.log.info(`Completed aggregation: ${present() - total_time} ms`);

  unwindStack(ctx.process, 'runAggregate');

  return attributeLookupFile;
};

// percent of features that will be retained at each zoom level
function getRetained() {
  return {
    '4': 0.15,
    '5': 0.22,
    '6': 0.29,
    '7': 0.36,
    '8': 0.43,
    '9': 0.5,
    '10': 0.6,
    '11': 0.7,
    '12': 0.8,
    '13': 0.9,
    '14': 1,
  };

  // TODO another one of these for Alaska ZoomLevels
}
