const { addUniqueIdNdjson } = require('../util/processGeoFile');

async function main() {
  const ctx = { log: console, process: [] };

  const duplicates = await addUniqueIdNdjson(ctx, './test/ElPaso', './test/ELPasoOutput');

  // console.log(duplicates);
}

main();
