// @ts-check

const fs = require('fs');
const { directories } = require('./constants');
const { moveFile } = require('./filesystemUtil');

exports.doBasicCleanup = async function (dirs, silent) {
  // move all files from given array dirs into processed

  const movedFiles = [];

  dirs.forEach(dir => {
    fs.readdir(dir, (err, files) => {
      if (err) throw err;

      const filteredFiles = files.filter(file => {
        return !file.includes('gitignore');
      });

      filteredFiles.forEach(file => {
        movedFiles.push(moveFile(`${dir}/${file}`, `${directories.processedDir}/${file}`));
      });
    });
  });

  await Promise.all(movedFiles);

  if (!silent) {
    dirs.forEach(dir => {
      console.log(`all files from '${dir}' moved to '${directories.processedDir}'.`);
    });

    console.log('Done moving files.\n');
  }
};
