// @ts-check

const fs = require('fs');
const unzipper = require('unzipper');
var archiver = require('archiver');
const path = require('path');
const { directories } = require('./constants');

exports.moveFile = function (oldPath, newPath) {
  return new Promise((resolve, reject) => {
    fs.rename(oldPath, newPath, err => {
      if (err) {
        if (err.code === 'EXDEV') {
          copy();
        } else {
          return reject(err);
        }
      }
      return resolve();
    });

    function copy() {
      var readStream = fs.createReadStream(oldPath);
      var writeStream = fs.createWriteStream(newPath);

      readStream.on('error', err => {
        return reject(err);
      });
      writeStream.on('error', err => {
        return reject(err);
      });
      readStream.on('close', function () {
        fs.unlink(oldPath, () => {
          return resolve();
        });
      });

      readStream.pipe(writeStream);
    }
  });
};

exports.extractZip = function (filePath) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(unzipper.Extract({ path: directories.unzippedDir }))
      .on('error', err => {
        console.error(`Error unzipping file: ${filePath}`);
        console.error(err);
        return reject(err);
      })
      .on('close', () => {
        console.log(`Finished unzipping file: ${filePath}`);
        return resolve();
      });
  });
};

// todo dont know where else to put this function
// determine if the unzipped folder contains a shapefile or FGDB
exports.checkForFileType = function () {
  return new Promise((resolve, reject) => {
    const arrayOfFiles = fs.readdirSync(directories.unzippedDir);

    console.log({ arrayOfFiles });

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

exports.zipShapefile = async function (outputPath, productKeySHP) {
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
      console.log(archive.pointer() + ' total bytes');
      console.log('archiver has been finalized and the output file descriptor has closed.');
      resolve();
    });

    // This event is fired when the data source is drained no matter what was the data source.
    // It is not part of this library but rather from the NodeJS Stream API.
    // @see: https://nodejs.org/api/stream.html#stream_event_end
    output.on('end', function () {
      console.log('Data has been drained');
    });

    // good practice to catch warnings (ie stat failures and other non-blocking errors)
    archive.on('warning', function (err) {
      if (err.code === 'ENOENT') {
        console.log('warning1');
        console.log(err);
      } else {
        console.log('warning2');
        console.log(err);
      }
    });

    archive.on('error', function (err) {
      console.error(err);
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

exports.getMaxDirectoryLevel = function (dir) {
  const directories = fs
    .readdirSync(dir, { withFileTypes: true })
    .filter(dirent => dirent.isDirectory())
    .map(dirent => parseInt(dirent.name));

  console.log({ directories });

  console.log(Math.max(...directories));
  return Math.max(...directories);
};
