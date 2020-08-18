// @ts-check

const fs = require('fs');
const zlib = require('zlib');
const turf = require('@turf/turf');
const { clustersKmeans } = require('../util/modKmeans.js');
const { idPrefix, zoomLevels, directories, clusterPrefix } = require('../util/constants');
const { unwindStack } = require('../util/misc');

// const tilesDir = `${directories.tilesDir + ctx.directoryId}/${dirName}`;

exports.clusterAggregated = async function (ctx, tilesDir, featureProperties) {
  ctx.process.push('clusterAggregated');

  // make attributes directory
  fs.mkdirSync(`${tilesDir}`);

  fs.mkdirSync(`${tilesDir}/featureAttributes`);

  const CLUSTER_SIZE = 500; // todo 5000
  const cluster_obj = {};
  const hulls = [];
  const totalClusters = [];

  // per each cluster, cluster again
  const ids = {};

  // TODO save last file per pattern
  // const tr9 = require(`./geojson/output.json`);

  console.log('all geojson has been loaded');

  for (let currentZoom = zoomLevels.LOW; currentZoom < zoomLevels.HIGH; currentZoom++) {
    console.log(
      'reading: ' +
        `${directories.productTempDir + ctx.directoryId}/aggregated_${currentZoom}.json`,
    );
    let geofile = JSON.parse(
      fs.readFileSync(
        `${directories.productTempDir + ctx.directoryId}/aggregated_${currentZoom}.json`,
        'utf8',
      ),
    );

    const filtered = geofile.features.filter(feature => {
      const exists = ids[feature.properties[idPrefix]];
      ids[feature.properties[idPrefix]] = true;
      return !exists;
    });

    const count = filtered.length;

    console.log(`counted ${count} features`);

    const point_array = filtered.map(feature => {
      return turf.centroid(feature.geometry, feature.properties);
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
      feature.properties = { __cluster: cluster_obj[feature.properties[idPrefix]] };
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

  totalClusters.forEach(cluster => {
    const obj = {};
    console.log('building feature file for cluster: ' + cluster);
    Object.keys(cluster_obj).forEach(key => {
      if (cluster_obj[key] === cluster) {
        obj[key] = featureProperties[key];
        delete obj[key][idPrefix];
        delete obj[key][clusterPrefix];
      }
    });
    const buffer = zlib.gzipSync(JSON.stringify(obj));
    fs.writeFileSync(`${tilesDir}/featureAttributes/${cluster}.json`, buffer);
  });

  // gzip and write into tile directory
  const buffer = zlib.gzipSync(JSON.stringify(turf.featureCollection(hulls)));

  fs.writeFileSync(`${tilesDir}/feature_hulls.geojson`, buffer.toString('base64'));

  unwindStack(ctx.process, 'clusterAggregated');
};
