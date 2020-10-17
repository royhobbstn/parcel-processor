const fs = require('fs');
const data = require('./new_england.json');
const popFile = require('./countySubPop2010.json');

const file = [];
const lookup = [];
const pop = [];

data.features.forEach(row => {
  file.push(`("${row.properties.NAME}", "${row.properties.GEOID}", "060")`);
  lookup.push(`'${row.properties.GEOID}': '${row.properties.NAME}'`);
});

popFile.forEach(row => {
  pop.push(`'${row.GEO_ID.slice(9)}': ${row.P002001}`);
});

fs.writeFileSync('./countySubRows.txt', file.join(',\n'), 'utf8');
fs.writeFileSync('./countySubLookup.txt', lookup.join(',\n'), 'utf8');
fs.writeFileSync('./countySubPop.txt', pop.join(',\n'), 'utf8');
