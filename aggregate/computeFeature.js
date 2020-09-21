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

  let line1;
  let l2;
  let area;

  try {
    line1 = turf.polygonToLine(feature);
    l2 = turf.length(feature);
    area = turf.area(feature);
  } catch (err) {
    console.warn('skipping feature.', { error: err.message });
  }

  nearby.features.slice(0, 50).forEach(near_feature => {
    if (near_feature.properties[idPrefix] === feature.properties[idPrefix]) {
      // ignore self
      return;
    }

    let line2;
    let intersection;

    try {
      line2 = turf.polygonToLine(near_feature);
      intersection = turf.lineOverlap(line1, line2);
    } catch (err) {
      // no big deal.  skipping potential match
      return;
    }

    // potentially could be within bbox but not intersecting
    if (intersection) {
      if (!intersection.features.length) {
        return;
      }

      const l1 = turf.length(intersection);

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
