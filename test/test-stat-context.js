const ndjson = require('ndjson');
const fs = require('fs');
const { StatContext } = require('../util/StatContext');
const statCounter = new StatContext({ log: console, process: [] });

let transformed = 0;

fs.createReadStream(`./elpaso.ndgeojson`)
  .pipe(ndjson.parse())
  .on('data', function (obj) {
    statCounter.countStats(obj);

    transformed++;
    if (transformed % 1000 === 0) {
      // console.log(obj);
      console.log(transformed + ' records processed');
    }
  })
  .on('error', err => {
    console.error(err);
  })
  .on('end', end => {
    console.log(transformed + ' records processed');
    console.log(statCounter.export());
    // fs.writeFileSync(`stat-elpaso.json`, JSON.stringify(statCounter.export()), 'utf8');
  });
