const fs = require('fs');
const { rawDir, processedDir } = require('./constants');
const { moveFile } = require('./filesystemUtil');

exports.doBasicCleanup = function () {
  // move all files from raw into processed
  fs.readdir(rawDir, (err, files) => {
    if (err) throw err;

    const filteredFiles = files.filter(file => {
      return !file.includes('gitignore');
    });

    const movedFiles = filteredFiles.map(file => {
      moveFile(`${rawDir}/${file}`, `${processedDir}/${file}`);
    });

    Promise.all(movedFiles).then(() => {
      console.log(`all files from '${rawDir}' moved to '${processedDir}'.  Done.\n`);
    });
  });
};
