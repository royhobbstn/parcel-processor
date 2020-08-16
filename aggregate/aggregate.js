// node --max_old_space_size=8192 aggregate.js bg 2016
// (geotype, zoomlevel, year)
// required: nodeJS 8+ for util.promisify

const fs = require('fs');
const geojsonRbush = require('geojson-rbush').default;
const { computeFeature } = require('./computeFeature.js');
const present = require('present');
const turf = require('@turf/turf');
const { idPrefix } = require('../util/constants');
const { promisify } = require('util');
const writeFileAsync = promisify(fs.writeFile);

const geojson_file = require(`./input.json`);

// TODO this is temporary
geojson_file.features.forEach((feature, index) => {
  feature.properties[idPrefix] = index + 1;
});

let geojson_feature_count = geojson_file.features.length;
console.log(`Features in dataset: ${geojson_feature_count}`);

/*** Mutable Globals ***/

let ordered_arr = [];
const keyed_geojson = {};
let counter = 0;
const threshold = [];
const written_file_promises = [];

/*** Initial index creation and calculation ***/

const tree = geojsonRbush();
tree.load(geojson_file);

geojson_file.features.forEach((feature, index) => {
  if (index % 2000 === 0) {
    console.log('index progress: ' + ((index / geojson_feature_count) * 100).toFixed(2) + '%');
  }

  keyed_geojson[feature.properties[idPrefix]] = Object.assign({}, feature, {
    properties: { [idPrefix]: feature.properties[idPrefix] },
  });
  computeFeature(feature, tree, ordered_arr, counter);
});

/********* main ************/

// continually combine smaller features.
// without aggregating across state or county lines
// note: this is beautiful

const total_time = present(); // tracks total execution time

const RETAINED = getRetained();

const LOW_ZOOM = 4;
const HIGH_ZOOM = 16;

/****** Setup ******/

const STARTING_GEOJSON_FEATURE_COUNT = geojson_feature_count;

// set an array of feature thresholds
for (let i = LOW_ZOOM; i < HIGH_ZOOM; i++) {
  threshold.push({ count: Math.round(geojson_feature_count * RETAINED[i]), zoom: i });
}

const DESIRED_NUMBER_FEATURES = Math.round(geojson_feature_count * RETAINED[LOW_ZOOM]);
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
      console.log('writing zoomlevel: ' + obj.zoom);
      written_file_promises.push(
        writeFileAsync(
          `./output_${obj.zoom}.json`,
          JSON.stringify(turf.featureCollection(geojson_array)),
          'utf8',
        ),
      );
    }
  });

  if (geojson_feature_count % 500 === 0) {
    console.log({ STARTING_GEOJSON_FEATURE_COUNT, geojson_feature_count });
    const progress =
      ((STARTING_GEOJSON_FEATURE_COUNT - geojson_feature_count) / REDUCTIONS_NEEDED) * 100;
    console.log(`compute progress ${progress.toFixed(2)} %`);
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
    computeFeature(combined, tree, ordered_arr, counter);
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
    `./output_${LOW_ZOOM}.json`,
    JSON.stringify(turf.featureCollection(geojson_array)),
    'utf8',
  ),
);

Promise.all(written_file_promises)
  .then(() => {
    // end of program
    console.log('Completed.');
    console.log(present() - total_time);
  })
  .catch(err => {
    // error writing file(s).  stop immediately
    console.log(err);
    process.exit();
  });

/*** Functions ***/

// percent of features that will be retained at each zoom level
function getRetained() {
  return {
    '4': 0.15,
    '5': 0.2,
    '6': 0.25,
    '7': 0.3,
    '8': 0.35,
    '9': 0.4,
    '10': 0.45,
    '11': 0.5,
    '12': 0.6,
    '13': 0.7,
    '14': 0.8,
    '15': 0.9,
  };

  // TODO another one of these for Alaska ZoomLevels
}
