const fs = require('fs');
const json = require('./us_parcel_areas.json');

json.features.forEach(feature => {
  if (feature.properties.COUSUBFP) {
    feature.properties.SUMLEV = '060';
  } else {
    feature.properties.SUMLEV = '050';
  }
});

fs.writeFileSync('./us_parcel_areas.geojson', JSON.stringify(json), 'utf8');
