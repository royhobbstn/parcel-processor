const chokidar = require('chokidar');
const crypto = require('crypto');
const fs = require('fs');
const unzipper = require('unzipper');
const { processFile } = require('./util/processFile');
const { rawDir, unzippedDir } = require('./util/constants');
const { queryHash, writePageCheck } = require('./util/queries');

// watch filesystem and compute hash of incoming file.
chokidar
  .watch(rawDir, {
    ignored: /(^|[\/\\])\../, // ignore dotfiles
    awaitWriteFinish: true,
  })
  .on('add', path => {
    console.log(`\nFile found: ${path}`);
    console.log('processing...');

    const hash = crypto.createHash('md5');
    const stream = fs.createReadStream(path);

    stream.on('data', data => {
      hash.update(data, 'utf8');
    });

    stream.on('end', () => {
      computedHash = hash.digest('hex');
      console.log(`Computed Hash: ${computedHash}`);

      checkDBforHash(path, computedHash);
    });
  });

console.log('listening...\n');

function checkDBforHash(path, computedHash) {
  // todo write page_check record to serverless aurora
  // writePageCheck()
  //   .then(result => {
  //     console.log(result);
  //   })
  //   .catch(err => {
  //     console.log(err);
  //   });

  // todo call serverless aurora and find out if hash is already in DB
  queryHash()
    .then(result => {
      console.log(result);
    })
    .catch(err => {
      console.log(err);
    });

  // if is in database, END

  // if not in database write a record in the download table

  // then extract
  // extractZip(path, computedHash);
}

function extractZip(path, computedHash) {
  fs.createReadStream(path)
    .pipe(unzipper.Extract({ path: unzippedDir }))
    .on('error', err => {
      console.error(`Error unzipping file: ${path}`);
      console.error(err);
      process.exit();
    })
    .on('close', () => {
      console.log(`Finished unzipping file: ${path}`);
      checkForFileType();
    });
}

// determine if the unzipped folder contains a shapefile or FGDB
function checkForFileType() {
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
    console.error('ERROR: mix of shapefiles and geodatabases in raw folder.  Exiting.');
    process.exit();
  }

  if (gdbFilenames.size === 1) {
    processFile(Array.from(gdbFilenames)[0], 'geodatabase');
  }

  if (gdbFilenames.size > 1) {
    // TODO multiple geodatabases
    console.error('ERROR: multiple geodatabases in raw folder.  Exiting.');
    process.exit();
  }

  if (shpFilenames.size === 1) {
    processFile(Array.from(shpFilenames)[0], 'shapefile');
  }

  if (shpFilenames.size > 1) {
    // TODO multiple shapefiles
    console.error('ERROR: multiple shapefiles in raw folder.  Exiting.');
    process.exit();
  }

  if (shpFilenames.size + gdbFilenames.size === 0) {
    console.error('Unknown filetypes in raw folder.  Nothing will be processed.');
  }
}
