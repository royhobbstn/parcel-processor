const fs = require('fs');
const unzipper = require('unzipper');
const { unzippedDir } = require('./constants');

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
      .pipe(unzipper.Extract({ path: unzippedDir }))
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
    const arrayOfFiles = fs.readdirSync(unzippedDir);

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
