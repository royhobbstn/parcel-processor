const { addUniqueIdNdjson } = require('../util/processGeoFile');
const present = require('present');

async function main() {
  const ctx = { log: console, process: [] };

  const time = present();
  const duplicates = await addUniqueIdNdjson(ctx, './test/Summit', './test/SummitOutput');

  console.log(present() - time);
}

main();

// 481173;
// 463427;
// 533797;
