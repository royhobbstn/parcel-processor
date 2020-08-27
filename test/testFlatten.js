const { addUniqueIdNdjson } = require('../util/processGeoFile');

async function main() {
  const ctx = { log: console, process: [] };

  const duplicates = await addUniqueIdNdjson(ctx, './test/48151', './test/wId');

  console.log(duplicates);
}

main();
