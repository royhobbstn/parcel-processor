// @ts-check

const turf = require('@turf/turf');
const { idPrefix } = require('../util/constants');

// passed by reference.  will mutate

exports.computeFeature = function (feature, tree, ordered_arr) {
  // console.log('computing: ' + feature.properties[idPrefix]);
  const bbox = turf.bbox(feature);
  const nearby = tree.search(bbox);
  // console.log('found nearby: ' + nearby.features.length);
  const best_match = {
    coalescability: Infinity,
    match: [],
  };

  // I'm slicing here because its possible there could be thousands of
  // overlapping features (rem: Denver condos)
  nearby.features.slice(0, 10).forEach((near_feature, i) => {
    // console.log('nearby feature: ' + i);
    if (near_feature.properties[idPrefix] === feature.properties[idPrefix]) {
      // ignore self
      return;
    }
    const line1 = turf.polygonToLine(feature);
    const line2 = turf.polygonToLine(near_feature);
    // console.log('polygon to line complete');
    const intersection = turf.lineOverlap(line1, line2);
    // console.log('intersection complete');

    // potentially could be within bbox but not intersecting
    if (intersection) {
      if (!intersection.features.length) {
        return;
      }
      // console.log('found intersection');

      const l1 = turf.length(intersection, { units: 'kilometers' });
      const l2 = turf.length(feature, { units: 'kilometers' });
      // console.log('intersection lengths');
      const area = turf.area(feature);
      const matching_feature_area = turf.area(near_feature);
      // console.log('intersection areas');

      const inverse_shared_edge = 1 - l1 / l2;
      const combined_area = area + matching_feature_area;

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
    // console.log('found best match');
    inOrder(ordered_arr, best_match);
    // console.log('put in order');
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
