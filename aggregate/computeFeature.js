// @ts-check

const turf = require('@turf/turf');
const { idPrefix } = require('../util/constants');

// passed by reference.  will mutate
exports.computeFeature = function (ctx, feature, tree, queue) {
  const nearby = tree.search(feature.bbox);
  const best_match = {
    coalescability: Infinity,
    match: [],
  };

  // if (nearby.features.length > 50) {
  //   ctx.log.info('trimming results list to 50 features');
  // }

  nearby.features.slice(0, 50).forEach(near_feature => {
    if (near_feature.properties[idPrefix] === feature.properties[idPrefix]) {
      // ignore self
      return;
    }
    const line1 = turf.polygonToLine(feature);
    const line2 = turf.polygonToLine(near_feature);
    const intersection = turf.lineOverlap(line1, line2);

    // potentially could be within bbox but not intersecting
    if (intersection) {
      if (!intersection.features.length) {
        return;
      }

      const l1 = turf.length(intersection);
      const l2 = turf.length(feature);
      const area = turf.area(feature);
      const matching_feature_area = turf.area(near_feature);

      const pow = Math.pow(l1 / l2, 2);
      const inverse_shared_edge = 1.0001 - pow;

      const combined_area = Math.sqrt(area + matching_feature_area);

      let coalescability = inverse_shared_edge * combined_area;

      // we only care about registering the best match; coalescability will
      // be recalculated as soon as a feature is joined to another,
      // rendering a lesser match useless
      if (coalescability < best_match.coalescability) {
        best_match.coalescability = coalescability;
        best_match.match = [feature.properties[idPrefix], near_feature.properties[idPrefix]];
      }
    }
  });

  if (best_match.match.length) {
    queue.push(best_match);
  }
};
