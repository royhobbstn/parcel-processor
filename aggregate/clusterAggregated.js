// @ts-check

const fs = require('fs');
const zlib = require('zlib');
const turf = require('@turf/turf');
const { clustersKmeans } = require('../util/modKmeans.js');
const { idPrefix, zoomLevels, directories, clusterPrefix } = require('../util/constants');
const { unwindStack } = require('../util/misc');
const ndjson = require('ndjson');

// const tilesDir = `${directories.tilesDir + ctx.directoryId}/${dirName}`;

exports.clusterAggregated = async function (
  ctx,
  tilesDir,
  featureProperties,
  derivativePath,
  fieldMetadata,
) {
  ctx.process.push('clusterAggregated');

  // make attributes directory
  fs.mkdirSync(`${tilesDir}`);

  fs.mkdirSync(`${tilesDir}/featureAttributes`);

  const CLUSTER_SIZE = 5000;
  const cluster_obj = {};
  const hulls = [];
  const totalClusters = [];
  const ids = {};

  for (
    let currentZoom = zoomLevels.LOW;
    currentZoom <= zoomLevels.HIGH;
    currentZoom = currentZoom + 2
  ) {
    const streamOriginalFile = currentZoom === zoomLevels.HIGH;

    let filtered = [];
    let filename;

    if (!streamOriginalFile) {
      // get from aggregated zoomlevel file
      filename = `${directories.productTempDir + ctx.directoryId}/aggregated_${currentZoom}.json`;
      ctx.log.info(`reading: ${filename}`);
    } else {
      // get from original ndgeojson
      ctx.log.info('Streaming original file for last aggregate diff.');
      ctx.log.info(`path: ${derivativePath}.ndgeojson`);
      filename = `${derivativePath}.ndgeojson`;
    }

    await new Promise((resolve, reject) => {
      fs.createReadStream(filename)
        .pipe(ndjson.parse())
        .on('data', function (obj) {
          const exists = ids[obj.properties[idPrefix]];
          ids[obj.properties[idPrefix]] = true;
          if (!exists) {
            filtered.push(obj);
          }
        })
        .on('error', err => {
          ctx.log.error('Error', { err: err.message, stack: err.stack });
          return reject(err);
        })
        .on('end', async () => {
          ctx.log.info(' records read and indexed');
          return resolve();
        });
    });

    const count = filtered.length;

    console.log(`counted ${count} features for zoomLevel ${currentZoom}`);

    const point_array = filtered.map(feature => {
      return turf.centroid(feature.geometry, { properties: feature.properties });
    });

    const point_layer = turf.featureCollection(point_array);

    ctx.log.info(`clusterAggregated level ${currentZoom}`);

    // Cluster the point files
    ctx.log.info(`clustering @ ${CLUSTER_SIZE}`);
    const clustered = clustersKmeans(ctx, point_layer, {
      numberOfClusters: Math.round(count / CLUSTER_SIZE) || 1,
    });

    ctx.log.info('done clustering using kmeans');

    const all_clusters = new Set();

    clustered.features.forEach(feature => {
      cluster_obj[feature.properties[idPrefix]] = `${currentZoom}_${feature.properties.cluster}`;
      all_clusters.add(`${currentZoom}_${feature.properties.cluster}`);
    });

    filtered.forEach(feature => {
      feature.properties.__cluster = cluster_obj[feature.properties[idPrefix]];
    });

    // for each cluster in the above list, create hull
    all_clusters.forEach(cluster => {
      const c_geojson = filtered.filter(feat => {
        return feat.properties.__cluster === cluster;
      });
      const c_layer = turf.featureCollection(c_geojson);
      const hull = turf.convex(c_layer);
      hull.properties = { cluster };

      hulls.push(hull);

      totalClusters.push(cluster);
    });
  }

  for (const cluster of totalClusters) {
    const obj = {};
    console.log('building feature file for cluster: ' + cluster);
    Object.keys(cluster_obj).forEach(key => {
      if (cluster_obj[key] === cluster) {
        for (let attr of Object.keys(featureProperties[key])) {
          if (attr === idPrefix || attr === clusterPrefix) {
            continue;
          }
          if (
            !Object.keys(fieldMetadata.numeric).includes(attr) &&
            !Object.keys(fieldMetadata.categorical).includes(attr)
          ) {
            // only create file for attributes that are mappable
            // we previously crunched stat file to determine elgibility
            continue;
          }
          if (!obj[attr]) {
            obj[attr] = {};
          }
          obj[attr][key] = featureProperties[key][attr];
        }
      }
    });

    for (let attr of Object.keys(obj)) {
      const buffer = zlib.gzipSync(JSON.stringify(obj[attr]));
      fs.writeFileSync(`${tilesDir}/featureAttributes/${cluster}__${attr}.json`, buffer);
    }
  }

  // gzip and write into tile directory
  ctx.log.info('writing feature_hulls.geojson');
  const buffer = zlib.gzipSync(JSON.stringify(turf.featureCollection(hulls)));
  fs.writeFileSync(`${tilesDir}/feature_hulls.geojson`, buffer);

  unwindStack(ctx.process, 'clusterAggregated');
};
