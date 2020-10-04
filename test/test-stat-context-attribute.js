const ndjson = require('ndjson');
const fs = require('fs');
const { StatContextAttribute } = require('../util/StatContextAttribute');

async function main() {
  const statCounter = new StatContextAttribute(
    { log: console, process: [] },
    `./california.ndgeojson`,
  );

  await statCounter.init();

  fs.writeFileSync('./californnia-stat.json', JSON.stringify(statCounter.exportStats()));
}

main().catch(err => console.log(err));
