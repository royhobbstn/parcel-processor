const { doBasicCleanup } = require('../util/cleanup');
const { directories } = require('../util/constants');

main();

async function main() {
  await doBasicCleanup(
    { log: console },
    [directories.rawDir, directories.outputDir, directories.unzippedDir],
    false,
  );
}
