// @ts-check

const turf = require('@turf/turf');
const { idPrefix } = require('../util/constants');

// passed by reference.  will mutate

exports.computeFeature = function (feature, tree, ordered_arr, counter) {
  const bbox = turf.bbox(feature);
  const nearby = tree.search(bbox);

  // TODO performance is there a better way to do this?
  const nearby_filtered = nearby.features.filter(d => {
    // ignore self
    const self = d.properties[idPrefix] === feature.properties[idPrefix];
    return !self;
  });

  const best_match = {
    coalescability: Infinity,
    match: [],
  };

  nearby_filtered.forEach(near_feature => {
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

      const inverse_shared_edge = 1 - l1 / l2;
      const combined_area = area + matching_feature_area;

      counter++;

      const coalescability = inverse_shared_edge * combined_area;

      // we only care about registering the best match; coalescibility will
      // be recalculated as soon as a feature is joined to another,
      // rendering a lesser match useless
      if (coalescability < best_match.coalescability) {
        best_match.coalescability = coalescability;
        best_match.match = [feature.properties[idPrefix], near_feature.properties[idPrefix]];
      }
    }
  });

  if (best_match.match.length) {
    inOrder(ordered_arr, best_match);
  }
};

function inOrder(arr, item) {
  /* Insert item into arr keeping low to high order */

  let ix = 0;
  while (ix < arr.length) {
    if (item.coalescability < arr[ix].coalescability) {
      break;
    }
    ix++;
  }

  arr.splice(ix, 0, item);
}
