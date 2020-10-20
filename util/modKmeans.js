// @ts-check

// modified version of turfJS kMeans to use random starting seeds
const turf = require('@turf/turf');
const skmeans = require('skmeans');
const { unwindStack, getTimestamp } = require('./misc');

exports.clustersKmeans = function (ctx, points, options) {
  console.log({ points, options });
  ctx.process.push({ name: 'clustersKmeans', timestamp: getTimestamp() });

  // Optional parameters
  options = options || {};
  if (typeof options !== 'object') throw new Error('options is invalid');
  var numberOfClusters = options.numberOfClusters;
  var mutate = options.mutate;

  // Input validation
  turf.invariant.collectionOf(points, 'Point', 'Input must contain Points');

  // Default Params
  var count = points.features.length;
  numberOfClusters = numberOfClusters || Math.round(Math.sqrt(count / 2));

  // numberOfClusters can't be greater than the number of points
  // fallbacks to count
  if (numberOfClusters > count) numberOfClusters = count;

  // Clone points to prevent any mutations (enabled by default)
  if (mutate === false || mutate === undefined) points = turf.clone(points, true);

  // collect points coordinates
  var data = turf.meta.coordAll(points);

  console.log({ data: data.length });
  console.log({ numberOfClusters });
  // create skmeans clusters
  var skmeansResult = skmeans(data, numberOfClusters, 'kmrand');

  // store centroids {clusterId: [number, number]}
  var centroids = {};
  skmeansResult.centroids.forEach(function (coord, idx) {
    centroids[idx] = coord;
  });

  // add associated cluster number
  turf.meta.featureEach(points, function (point, index) {
    var clusterId = skmeansResult.idxs[index];
    point.properties.cluster = clusterId;
    point.properties.centroid = centroids[clusterId];
  });

  unwindStack(ctx, 'clustersKmeans');
  return points;
};
