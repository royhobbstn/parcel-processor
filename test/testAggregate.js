const { runAggregate } = require('../aggregate/aggregate');
const { directories } = require('../util/constants');
const { createDirectories } = require('../util/filesystemUtil');
const present = require('present');

async function main() {
  const ctx = {
    log: console,
    process: [],
    directoryId: 'test-aggregate',
    timeBank: {},
    timeStack: [],
  };
  await createDirectories(ctx, [directories.productTempDir]);
  await runAggregate(ctx, './test/48151');
}

main();
