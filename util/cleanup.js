// @ts-check

const fs = require('fs');
const rimraf = require('rimraf');
const mkdirp = require('mkdirp');
const { directories } = require('./constants');
const { moveFile } = require('./filesystemUtil');

exports.doBasicCleanup = async function (ctx, dirs, cleanProcessedBool) {
  // move all files from given array dirs into processed
  if (cleanProcessedBool) {
    await new Promise((resolve, reject) => {
      rimraf(directories.processedDir, {}, err => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
    mkdirp.sync(directories.processedDir);
    fs.writeFileSync(`${directories.processedDir}/.gitignore`, '*\n!.gitignore\n');
  }

  await new Promise((resolve, reject) => {
    dirs.forEach(dir => {
      fs.readdir(dir, async (err, files) => {
        if (err) {
          return reject(err);
        }

        const filteredFiles = files.filter(file => {
          return !file.includes('gitignore');
        });

        const movedFiles = [];

        filteredFiles.forEach(file => {
          movedFiles.push(moveFile(`${dir}/${file}`, `${directories.processedDir}/${file}`));
        });

        await Promise.all(movedFiles);
        resolve();
      });
    });
  });

  dirs.forEach(dir => {
    ctx.log.info(`all files from '${dir}' moved to '${directories.processedDir}'.`);
  });

  ctx.log.info('Done moving files.\n');
};
