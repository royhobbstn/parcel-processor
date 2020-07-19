// @ts-check

const fs = require('fs');
const unzipper = require('unzipper');
const archiver = require('archiver');
const path = require('path');
const mkdirp = require('mkdirp');
const fsExtra = require('fs-extra');
const del = require('del');
const { directories } = require('./constants');
const { generateRef } = require('./crypto');
const { unwindStack } = require('./misc');

exports.createDirectories = async function (ctx, dirs) {
  ctx.process.push('createDirectories');

  for (let dir of dirs) {
    const newDir = `${dir}${ctx.directoryId}`;
    mkdirp.sync(newDir);
    ctx.log.info(`Created directory: ${newDir}`);
  }
  ctx.log.info('Done creating staging directories.');
  unwindStack(ctx.process, 'createDirectories');
};

exports.extractZip = function (ctx, filePath) {
  ctx.process.push('extractZip');

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(unzipper.Extract({ path: directories.unzippedDir + ctx.directoryId }))
      .on('error', err => {
        ctx.log.error(`Error unzipping file: ${filePath}`, { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('close', () => {
        ctx.log.info(`Finished unzipping file: ${filePath}`);
        unwindStack(ctx.process, 'extractZip');
        return resolve();
      });
  });
};

exports.collapseUnzippedDir = collapseUnzippedDir;

function collapseUnzippedDir(ctx) {
  ctx.process.push('collapseUnzippedDir');

  const root = directories.unzippedDir + ctx.directoryId;
  const arrayOfFiles = fs.readdirSync(root);
  let movedFlag = false;

  for (let file of arrayOfFiles) {
    const isDir = fs
      .lstatSync(`${directories.unzippedDir + ctx.directoryId}/${file}`)
      .isDirectory();
    const isGDB = file.slice(-4).toLowerCase() === '.gdb';

    if (isDir && !isGDB) {
      // move contents of this directory to root
      const subDirectory = `${directories.unzippedDir + ctx.directoryId}/${file}`;
      ctx.log.info(`Moving contents of folder: ${subDirectory} into base folder: ${root}`);
      const arrayOfSubDirectoryFiles = fs.readdirSync(subDirectory);

      // add move-prefix to avoid potential filename collision with identical files (or identically named files) in the lower directory
      const prefix = generateRef(ctx, 5);
      for (let subFile of arrayOfSubDirectoryFiles) {
        fsExtra.moveSync(`${subDirectory}/${subFile}`, `${root}/${prefix}-${subFile}`);
        movedFlag = true;
      }
    }
  }

  if (movedFlag) {
    unwindStack(ctx.process, 'collapseUnzippedDir');
    return collapseUnzippedDir;
  }

  unwindStack(ctx.process, 'collapseUnzippedDir');
}

// todo dont know where else to put this function
// determine if the unzipped folder contains a shapefile or FGDB
exports.checkForFileType = function (ctx) {
  ctx.process.push('checkForFileType');

  return new Promise((resolve, reject) => {
    const arrayOfFiles = fs.readdirSync(directories.unzippedDir + ctx.directoryId);

    ctx.log.info('unzipped directory files', { arrayOfFiles });

    // determine if it's a shapefile by examining files in directory and looking for .shp
    // noting that there could possibly be multiple shapefiles in a zip archive
    const shpFilenames = new Set();
    const gdbFilenames = new Set();

    arrayOfFiles.forEach(file => {
      if (file.endsWith('.shp')) {
        const filename = file.split('.shp')[0];
        shpFilenames.add(filename);
      }
      if (file.endsWith('.gdb')) {
        gdbFilenames.add(file);
      }
    });

    if (shpFilenames.size > 0 && gdbFilenames.size > 0) {
      ctx.log.warn(
        "There are both geodatabases and shapefiles in here. If something goes wrong, it's probably because I guessed and chose the wrong file.  I prefer geodatabases.",
      );
    }

    if (gdbFilenames.size === 1) {
      unwindStack(ctx.process, 'checkForFileType');
      return resolve([Array.from(gdbFilenames)[0], 'geodatabase']);
    }

    if (gdbFilenames.size > 1) {
      // TODO multiple geodatabases
      return reject(new Error('ERROR: multiple geodatabases in raw folder.  Exiting.'));
    }

    if (shpFilenames.size === 1) {
      unwindStack(ctx.process, 'checkForFileType');
      return resolve([Array.from(shpFilenames)[0], 'shapefile']);
    }

    if (shpFilenames.size > 1) {
      // TODO multiple shapefiles
      return reject(new Error('ERROR: multiple shapefiles in raw folder.  Exiting.'));
    }

    if (shpFilenames.size + gdbFilenames.size === 0) {
      return reject(new Error('Unknown filetypes in raw folder.  Nothing will be processed.'));
    }

    return reject(new Error('unknown state in checkForFileType'));
  });
};

exports.zipShapefile = async function (ctx, outputPath, productKeySHP) {
  ctx.process.push('zipShapefile');

  return new Promise((resolve, reject) => {
    const keyBase = path.parse(productKeySHP).base;
    // create a file to stream archive data to.
    var output = fs.createWriteStream(
      `${directories.outputDir + ctx.directoryId}/${keyBase}-shp.zip`,
    );
    var archive = archiver('zip', {
      zlib: { level: 9 }, // Sets the compression level.
    });

    // listen for all archive data to be written
    // 'close' event is fired only when a file descriptor is involved
    output.on('close', function () {
      ctx.log.info(archive.pointer() + ' total bytes');
      ctx.log.info('archiver has been finalized and the output file descriptor has closed.');
      unwindStack(ctx.process, 'zipShapefile');
      resolve();
    });

    // This event is fired when the data source is drained no matter what was the data source.
    // It is not part of this library but rather from the NodeJS Stream API.
    // @see: https://nodejs.org/api/stream.html#stream_event_end
    output.on('end', function () {
      ctx.log.info('Data has been drained');
    });

    // good practice to catch warnings (ie stat failures and other non-blocking errors)
    archive.on('warning', function (err) {
      if (err.code === 'ENOENT') {
        ctx.log.warn('warning1');
        ctx.log.warn(err);
      } else {
        ctx.log.warn('warning2');
        ctx.log.warn(err);
      }
    });

    archive.on('error', function (err) {
      ctx.log.error('Error', { err: err.message, stack: err.stack });
      reject(err);
    });

    // pipe archive data to the file
    archive.pipe(output);

    // append a file from stream
    // var file1 = __dirname + '/file1.txt';
    // archive.append(fs.createReadStream(file1), { name: 'file1.txt' });

    // append a file
    archive.file(`${outputPath}.dbf`, {
      name: `${keyBase}.dbf`,
    });
    archive.file(`${outputPath}.prj`, {
      name: `${keyBase}.prj`,
    });
    archive.file(`${outputPath}.shp`, {
      name: `${keyBase}.shp`,
    });
    archive.file(`${outputPath}.shx`, {
      name: `${keyBase}.shx`,
    });
    // finalize the archive (ie we are done appending files but streams have to finish yet)
    // 'close', 'end' or 'finish' may be fired right after calling this method so register to them beforehand
    archive.finalize();
  });
};

exports.getMaxDirectoryLevel = function (ctx, dir) {
  ctx.process.push('getMaxDirectoryLevel');

  const dirs = fs
    .readdirSync(dir, { withFileTypes: true })
    .filter(dirent => dirent.isDirectory())
    .map(dirent => parseInt(dirent.name));

  ctx.log.info('Directories', { dirs });

  ctx.log.info(Math.max(...dirs));

  unwindStack(ctx.process, 'zipShapefile');
  return Math.max(...dirs);
};

exports.cleanEFS = async function (ctx) {
  ctx.process.push('cleanEFS');

  const contents = fs.readdirSync(directories.root);

  for (let entry of contents) {
    const dir = path.join(directories.root, entry);
    const stats = fs.statSync(dir);

    const modifiedTime = stats.mtimeMs;
    const currentTime = new Date().getTime();
    const duration = (currentTime - modifiedTime) / 1000 / 60 / 60;

    ctx.log.info('clean efs dir check: ', { modifiedTime, currentTime, duration });

    // if older than a day
    if (duration > 24) {
      try {
        await del(dir, { force: true });
        ctx.log.info(`${dir} was old and is now deleted!`);
      } catch (err) {
        ctx.log.error(`Error while deleting ${dir}.`, { error: err.message, stack: err.stack });
      }
    }
  }

  unwindStack(ctx.process, 'cleanEFS');
};
