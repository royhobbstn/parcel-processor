// @ts-check

const turf = require('@turf/turf');
const { idPrefix } = require('../util/constants');

// passed by reference.  will mutate

exports.computeFeature = function (feature, tree, ordered_obj, counter) {
  const bbox = turf.bbox(feature);
  const nearby = tree.search(bbox);

  // console.log(nearby.features.length);
  const nearby_filtered = nearby.features.filter(d => {
    // ignore self
    const self = d.properties[idPrefix] === feature.properties[idPrefix];
    const blankId = !d.properties[idPrefix];

    return !self && !blankId;
  });
  // console.log(nearby_filtered.length);
  // console.log('filtered');

  const best_match = {
    coalescability: Infinity,
    match: [],
    geo_division: 'A',
  };

  // TODO new first find overlaps

  nearby_filtered.forEach(near_feature => {
    const line1 = turf.polygonToLine(feature);
    const line2 = turf.polygonToLine(near_feature);
    const intersection = turf.lineOverlap(line1, line2);

    // potentially could be within bbox but not intersecting
    if (intersection) {
      if (!intersection.features.length) {
        return;
      }
      // console.log('overlap');

      const l1 = turf.length(intersection, { units: 'kilometers' });
      const l2 = turf.length(feature, { units: 'kilometers' });
      const area = turf.area(feature);
      const matching_feature_area = turf.area(near_feature);

      const inverse_shared_edge = 1 - l1 / l2;
      const combined_area = area + matching_feature_area;
      const geo_division = 'A';

      counter++;

      const coalescability = inverse_shared_edge * combined_area;
      const c_counter = `_${counter}`;

      // we only care about registering the best match; coalescibility will
      // be recalculated as soon as a feature is joined to another,
      // rendering a lesser match useless
      if (coalescability < best_match.coalescability) {
        best_match.coalescability = coalescability;
        best_match.c_counter = c_counter;
        best_match.match = [feature.properties[idPrefix], near_feature.properties[idPrefix]];
        best_match.geo_division = geo_division;
      }
    } else {
      // console.log('no overlap');
    }
  });

  if (best_match.match.length) {
    if (!ordered_obj[best_match.geo_division]) {
      ordered_obj[best_match.geo_division] = [];
    }
    inOrder(ordered_obj[best_match.geo_division], best_match);
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
