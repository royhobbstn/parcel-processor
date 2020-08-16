// @ts-check

const fs = require('fs');
const turf = require('@turf/turf');
const zlib = require('zlib');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const { clustersKmeans } = require('../util/modKmeans.js');
const { idPrefix, zoomLevels } = require('../util/constants');

const GEO_BUCKET = 'v2-cluster-json';
const GEO_METADATA_BUCKET = 'geo-metadata';
const CLUSTER_SIZE = 200;
const cluster_obj = {};
const hulls = [];

const tr3 = require(`./aggregated-geojson/output_3.json`);
const tr4 = require(`./aggregated-geojson/output_4.json`);
const tr5 = require(`./aggregated-geojson/output_5.json`);
const tr6 = require(`./aggregated-geojson/output_6.json`);
const tr7 = require(`./aggregated-geojson/output_7.json`);
const tr8 = require(`./aggregated-geojson/output_8.json`);
const tr9 = require(`./geojson/output.json`);

console.log('all geojson has been loaded');

// per each cluster, cluster again
const ids = {};

for (let zoom = zoomLevels.LOW; zoom < zoomLevels.HIGH; zoom++) {}

tr3.features.forEach(feature => {
  ids[feature.properties[idPrefix]] = true;
});

const filtered_tr4 = tr4.features.filter(feature => {
  return !geoids.includes(feature.properties[idPrefix]);
});

console.log('filtered level 4');

filtered_tr4.forEach(feature => {
  geoids.push(feature.properties[idPrefix]);
});

const filtered_tr5 = tr5.features.filter(feature => {
  return !geoids.includes(feature.properties[idPrefix]);
});

filtered_tr5.forEach(feature => {
  geoids.push(feature.properties[idPrefix]);
});

console.log('filtered level 5');

const filtered_tr6 = tr6.features.filter(feature => {
  return !geoids.includes(feature.properties[idPrefix]);
});

filtered_tr6.forEach(feature => {
  geoids.push(feature.properties[idPrefix]);
});

console.log('filtered level 6');

const filtered_tr7 = tr7.features.filter(feature => {
  return !geoids.includes(feature.properties[idPrefix]);
});

filtered_tr7.forEach(feature => {
  geoids.push(feature.properties[idPrefix]);
});

console.log('filtered level 7');

const filtered_tr8 = tr8.features.filter(feature => {
  return !geoids.includes(feature.properties[idPrefix]);
});

filtered_tr8.forEach(feature => {
  geoids.push(feature.properties[idPrefix]);
});

console.log('filtered level 8');

const filtered_tr9 = tr9.features.filter(feature => {
  return !geoids.includes(feature.properties[idPrefix]);
});

filtered_tr9.forEach(feature => {
  geoids.push(feature.properties[idPrefix]);
});

console.log('filtered level 9');

// convert to point files

[
  { feature_array: tr3.features, zoom: 3 },
  { feature_array: filtered_tr4, zoom: 4 },
  { feature_array: filtered_tr5, zoom: 5 },
  { feature_array: filtered_tr6, zoom: 6 },
  { feature_array: filtered_tr7, zoom: 7 },
  { feature_array: filtered_tr8, zoom: 8 },
  { feature_array: filtered_tr9, zoom: 9 },
].forEach(obj => {
  const count = obj.feature_array.length;

  console.log(`counted ${count} features`);

  const point_array = obj.feature_array.map(feature => {
    return turf.centroid(feature.geometry, feature.properties);
  });

  var point_layer = turf.featureCollection(point_array);

  // Cluster the point files
  console.log(`clustering @ ${CLUSTER_SIZE}`);
  const clustered = clustersKmeans(point_layer, {
    numberOfClusters: Math.round(count / CLUSTER_SIZE) || 1,
  });

  console.log('done clustering');

  const all_clusters = new Set();

  clustered.features.forEach(feature => {
    cluster_obj[feature.properties[idPrefix]] = `${obj.zoom}_${feature.properties.cluster}`;
    all_clusters.add(`${obj.zoom}_${feature.properties.cluster}`);
  });

  const updated_features = obj.feature_array
    .map(feature => {
      const properties = Object.assign({}, feature.properties, {
        cluster: cluster_obj[feature.properties[idPrefix]],
      });
      return Object.assign(feature, { properties: properties });
    })
    .filter(feature => {
      // filter out null geography
      return feature.geometry;
    });

  // for each cluster in the above list
  all_clusters.forEach(cluster => {
    const c_geojson = updated_features.filter(feat => {
      return feat.properties.cluster === cluster;
    });
    const c_layer = turf.featureCollection(c_geojson);
    const hull = turf.convex(c_layer);
    hull.properties = { cluster };

    hulls.push(hull);
  });
});

// geoid_lookup better as { CLUSTER: [ARRAY OF GEOIDS] }.
const cluster_array = {};

Object.keys(cluster_obj).forEach(geoid => {
  const cluster = cluster_obj[geoid];
  if (!cluster_array[cluster]) {
    cluster_array[cluster] = [];
  }
  cluster_array[cluster].push(geoid);
});

// save geoid-cluster lookup to s3
const key = `clusters_lookup.json`;

// TODO use FS.

// zlib.gzip(JSON.stringify(cluster_array), function (error, result) {
//   if (error) throw error;

//   const params = {
//     Bucket: GEO_METADATA_BUCKET,
//     Key: key,
//     Body: result,
//     ContentType: 'application/json',
//     ContentEncoding: 'gzip',
//   };
//   s3.putObject(params, function (err, data) {
//     if (err) {
//       console.log(err);
//     } else {
//       console.log(`Successfully uploaded data to ${key}`);
//     }
//   });
// });

// save json to s3
const jsonfile_key = `clusters_output.json`;

// TODO use FS.

// zlib.gzip(JSON.stringify(hulls), function (error, result) {
//   if (error) throw error;

//   const params = {
//     Bucket: GEO_BUCKET,
//     Key: jsonfile_key,
//     Body: result,
//     ContentType: 'application/json',
//     ContentEncoding: 'gzip',
//   };
//   s3.putObject(params, function (err, data) {
//     if (err) {
//       console.log(err);
//     } else {
//       console.log(`Successfully uploaded data to ${GEO_BUCKET} ${key}`);
//     }
//   });
// });
