const ndjson = require('ndjson');
const fs = require('fs');
const { StatContext } = require('../util/StatContext');
const statCounter = new StatContext({ log: console });

let transformed = 0;

fs.createReadStream(`./5fe3a581-65aa145b-15-Hawaii.ndgeojson`)
  .pipe(ndjson.parse())
  .on('data', function (obj) {
    statCounter.countStats(obj);

    transformed++;
    if (transformed % 10000 === 0) {
      console.log(transformed + ' records processed');
    }
  })
  .on('error', err => {
    console.error(err);
  })
  .on('end', end => {
    console.log(transformed + ' records processed');
    fs.writeFileSync(`stat-context-test.json`, JSON.stringify(statCounter.export()), 'utf8');
  });
