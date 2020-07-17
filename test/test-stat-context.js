const ndjson = require('ndjson');
const fs = require('fs');
const { StatContext } = require('../util/StatContext');
const statCounter = new StatContext({ log: console, process: [] });

let transformed = 0;

fs.createReadStream(`./fileV12.nd`)
  .pipe(ndjson.parse())
  .on('data', function (obj) {
    // statCounter.countStats(obj);

    transformed++;
    if (transformed % 500000 === 0) {
      console.log(obj);
      console.log(transformed + ' records processed');
    }
  })
  .on('error', err => {
    console.error(err);
  })
  .on('end', end => {
    console.log(transformed + ' records processed');
    // fs.writeFileSync(`stat-context-test.json`, JSON.stringify(statCounter.export()), 'utf8');
  });
