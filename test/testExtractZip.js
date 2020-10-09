const { extractZip } = require('../util/filesystemUtil');

async function main() {
  const ctx = { log: console, process: [], timeBank: {}, timeStack: [] };
  extractZip(ctx, './file.zip');
}

main();
