const turf = require('@turf/turf');

const feature1 = {
  type: 'Feature',
  properties: { __po_id: 151935 },
  geometry: {
    type: 'Polygon',
    coordinates: [
      [
        [-106.4998424, 31.7969167],
        [-106.4998859, 31.796918],
        [-106.4998843, 31.7967103],
        [-106.4998284, 31.796707],
        [-106.4998298, 31.7967268],
        [-106.4998366, 31.7968292],
        [-106.4998424, 31.7969167],
      ],
    ],
  },
  bbox: [-106.4998859, 31.796707, -106.4998284, 31.796918],
};

const feature2 = {
  type: 'Feature',
  properties: { __po_id: 151870 },
  geometry: {
    type: 'Polygon',
    coordinates: [
      [
        [-106.4999089, 31.7967081],
        [-106.5003112, 31.7967141],
        [-106.500314, 31.7966612],
        [-106.4998955, 31.7966709],
        [-106.4998843, 31.7967103],
        [-106.4998284, 31.7967069],
        [-106.4998297, 31.7967268],
        [-106.4998048, 31.7967272],
        [-106.4998298, 31.7967268],
        [-106.4998284, 31.796707],
        [-106.4998843, 31.7967103],
        [-106.4999089, 31.7967081],
      ],
    ],
  },
  bbox: [-106.500314, 31.7966612, -106.4998048, 31.7967272],
};

console.log(turf.area(feature1));
console.log(turf.area(feature2));

console.log(turf.kinks(feature1));
console.log(JSON.stringify(turf.kinks(feature2)));

const buffer = turf.buffer(feature2, 0);
console.log(JSON.stringify(buffer));
console.log('---');
console.log(turf.kinks(buffer));
const unkink = turf.unkinkPolygon(buffer);
console.log(JSON.stringify(unkink));

const trunc = turf.truncate(unkink, { precision: 4, coordinates: 2, mutate: true });
console.log(JSON.stringify(trunc));
console.log(JSON.stringify(unkink));

console.log(
  turf.area({
    type: 'Feature',
    properties: { __po_id: 376461 },
    geometry: {
      type: 'Polygon',
      coordinates: [
        [
          [-106.6238, 31.8684],
          [-106.6238, 31.8684],
        ],
      ],
    },
    bbox: [-106.6238, 31.8684, -106.6238, 31.8684],
  }),
);
