const { addUniqueIdNdjson } = require('../util/processGeoFile');
const present = require('present');

async function main() {
  const ctx = { log: console, process: [] };

  const time = present();
  const duplicates = await addUniqueIdNdjson(ctx, './test/Denver', './test/DenverOutput');

  console.log(present() - time);
}

main();
