// @ts-check
const fs = require('fs');
const turf = require('@turf/turf');
const { idPrefix } = require('../util/constants');

// passed by reference.  will mutate

exports.computeFeature = function (feature, tree, ordered_arr) {
  const nearby = tree.search(feature.bbox);
  const best_match = {
    coalescability: Infinity,
    match: [],
  };

  // TODO check this count again to make sure multipart to singlepart and remove overlapping are working
  nearby.features.forEach(near_feature => {
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

      const l1 = turf.length(intersection, { units: 'kilometers' });
      const l2 = turf.length(feature, { units: 'kilometers' });
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
    // todo retry binary search at some point
    inOrder(ordered_arr, best_match);
  }
};

function inOrder(arr, item) {
  let ix = 0;
  while (ix < arr.length) {
    if (item.coalescability < arr[ix].coalescability) {
      break;
    }
    ix++;
  }

  arr.splice(ix, 0, item);
}
