// modified version of turfJS kMeans to use random starting seeds

var clone = require('@turf/clone');
var invariant = require('@turf/invariant');
var meta = require('@turf/meta');
var skmeans = require('skmeans');

exports.clustersKmeans = function (ctx, points, options) {
  ctx.process.push('clustersKmeans');

  // Optional parameters
  options = options || {};
  if (typeof options !== 'object') throw new Error('options is invalid');
  var numberOfClusters = options.numberOfClusters;
  var mutate = options.mutate;

  // Input validation
  invariant.collectionOf(points, 'Point', 'Input must contain Points');

  // Default Params
  var count = points.features.length;
  numberOfClusters = numberOfClusters || Math.round(Math.sqrt(count / 2));

  // numberOfClusters can't be greater than the number of points
  // fallbacks to count
  if (numberOfClusters > count) numberOfClusters = count;

  // Clone points to prevent any mutations (enabled by default)
  if (mutate === false || mutate === undefined) points = clone(points, true);

  // collect points coordinates
  var data = meta.coordAll(points);

  // create skmeans clusters
  var skmeansResult = skmeans(data, numberOfClusters, 'kmrand');

  // store centroids {clusterId: [number, number]}
  var centroids = {};
  skmeansResult.centroids.forEach(function (coord, idx) {
    centroids[idx] = coord;
  });

  // add associated cluster number
  meta.featureEach(points, function (point, index) {
    var clusterId = skmeansResult.idxs[index];
    point.properties.cluster = clusterId;
    point.properties.centroid = centroids[clusterId];
  });

  return points;
};
