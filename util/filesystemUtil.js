// @ts-check

const fs = require('fs');
const unzipper = require('unzipper');
var archiver = require('archiver');
const path = require('path');
const { directories } = require('./constants');

exports.moveFile = function (ctx, oldPath, newPath) {
  return new Promise((resolve, reject) => {
    fs.rename(path.normalize(oldPath), path.normalize(newPath), err => {
      if (err) {
        return reject(err);
      }
      return resolve();
    });
  });
};

exports.extractZip = function (ctx, filePath) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(unzipper.Extract({ path: directories.unzippedDir }))
      .on('error', err => {
        ctx.log.error(`Error unzipping file: ${filePath}`, { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('close', () => {
        ctx.log.info(`Finished unzipping file: ${filePath}`);
        return resolve();
      });
  });
};

// todo dont know where else to put this function
// determine if the unzipped folder contains a shapefile or FGDB
exports.checkForFileType = function (ctx) {
  return new Promise((resolve, reject) => {
    const arrayOfFiles = fs.readdirSync(directories.unzippedDir);

    ctx.log.info({ arrayOfFiles });

    // determine if it's a shapefile by examining files in directory and looking for .shp
    // noting that there could possibly be multiple shapefiles in a zip archive
    const shpFilenames = new Set();
    const gdbFilenames = new Set();

    arrayOfFiles.forEach(file => {
      if (file.includes('.shp')) {
        const filename = file.split('.shp')[0];
        shpFilenames.add(filename);
      }
      if (file.includes('.gdb')) {
        gdbFilenames.add(file);
      }
    });

    if (shpFilenames.size > 0 && gdbFilenames.size > 0) {
      return reject('ERROR: mix of shapefiles and geodatabases in raw folder.  Exiting.');
    }

    if (gdbFilenames.size === 1) {
      return resolve([Array.from(gdbFilenames)[0], 'geodatabase']);
    }

    if (gdbFilenames.size > 1) {
      // TODO multiple geodatabases
      return reject('ERROR: multiple geodatabases in raw folder.  Exiting.');
    }

    if (shpFilenames.size === 1) {
      return resolve([Array.from(shpFilenames)[0], 'shapefile']);
    }

    if (shpFilenames.size > 1) {
      // TODO multiple shapefiles
      return reject('ERROR: multiple shapefiles in raw folder.  Exiting.');
    }

    if (shpFilenames.size + gdbFilenames.size === 0) {
      return reject('Unknown filetypes in raw folder.  Nothing will be processed.');
    }

    return reject('unknown state in checkForFileType');
  });
};

exports.zipShapefile = async function (ctx, outputPath, productKeySHP) {
  return new Promise((resolve, reject) => {
    const keyBase = path.parse(productKeySHP).base;
    // create a file to stream archive data to.
    var output = fs.createWriteStream(`${directories.outputDir}/${keyBase}-shp.zip`);
    var archive = archiver('zip', {
      zlib: { level: 9 }, // Sets the compression level.
    });

    // listen for all archive data to be written
    // 'close' event is fired only when a file descriptor is involved
    output.on('close', function () {
      ctx.log.info(archive.pointer() + ' total bytes');
      ctx.log.info('archiver has been finalized and the output file descriptor has closed.');
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
  const directories = fs
    .readdirSync(dir, { withFileTypes: true })
    .filter(dirent => dirent.isDirectory())
    .map(dirent => parseInt(dirent.name));

  ctx.log.info({ directories });

  ctx.log.info(Math.max(...directories));
  return Math.max(...directories);
};
