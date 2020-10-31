const testFolder = './data/';
const fs = require('fs');

const geojsonFeatures = [];

fs.readdirSync(testFolder).forEach(file => {
  console.log(file);
  const geojson = JSON.parse(fs.readFileSync(testFolder + file));
  geojsonFeatures.push(...geojson.features);
});

const output = {
  type: 'FeatureCollection',
  features: geojsonFeatures,
};

fs.writeFileSync('lakeCountyOregon_10-31-20.geojson', JSON.stringify(output), 'utf8');
