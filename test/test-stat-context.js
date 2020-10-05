const fs = require('fs');
const { StatContext } = require('../util/StatContext');

async function main() {
  const statCounter = new StatContext({ log: console, process: [] }, `./Hartley.ndgeojson`);

  await statCounter.init();

  fs.writeFileSync('./hartley-stat.json', JSON.stringify(statCounter.exportStats()));
}

main().catch(err => console.log(err));
