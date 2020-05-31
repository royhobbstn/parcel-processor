const chokidar = require('chokidar');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const unzipper = require('unzipper');
const { processFile } = require('./util/processFile');
const {
  rawDir,
  unzippedDir,
  processedDir,
  rawBucket,
  productsBucket,
} = require('./util/constants');
const { putFileToS3 } = require('./util/s3Operations');
const {
  queryHash,
  queryPage,
  queryWritePage,
  queryWritePageCheck,
  queryCreateDownloadRecord,
  queryHealth,
} = require('./util/queries');
const { moveFile } = require('./util/filesystemUtil');
const prompt = require('prompt');

init().catch(err => {
  console.error('unexpected error');
  console.error(err);
});

// ping the database to make sure its up / get it ready
// after that, keep-alives from data-api-client should do the rest
async function init() {
  const seconds = 10;
  let connected = false;
  do {
    console.log('attempting to connect to database');
    connected = await checkHealth();
    if (!connected) {
      console.log(`attempt failed.  trying again in ${seconds} seconds...`);
      await setPause(seconds * 1000);
    }
  } while (!connected);

  console.log('connected');
  watchFilesystem();
}

function setPause(timer) {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve();
    }, timer);
  });
}

async function checkHealth() {
  try {
    await queryHealth();
    return true;
  } catch (e) {
    return false;
  }
}

// watch filesystem and compute hash of incoming file.
function watchFilesystem() {
  chokidar
    .watch(rawDir, {
      ignored: /(^|[\/\\])\../, // ignore dotfiles
      awaitWriteFinish: true,
    })
    .on('add', async filePath => {
      console.log(`\nFile found: ${filePath}`);

      let pageId;

      const pageNameInput = await pageInputPrompt();

      pageId = await fetchPageIdIfExists(pageNameInput);

      if (pageId === -1) {
        pageId = await createPage(pageNameInput);
      }

      const checkId = await recordPageCheck(pageId);

      console.log('processing file...');

      const hash = crypto.createHash('md5');
      const stream = fs.createReadStream(filePath);

      stream.on('data', data => {
        hash.update(data, 'utf8');
      });

      stream.on('end', () => {
        computedHash = hash.digest('hex');
        console.log(`Computed Hash: ${computedHash}`);

        checkDBforHash(filePath, computedHash, pageId, checkId);
      });
    });
  console.log('listening...\n');
}

async function recordPageCheck(pageId) {
  const query = await queryWritePageCheck(pageId);
  return query.insertId;
}

function pageInputPrompt() {
  return new Promise((resolve, reject) => {
    prompt.start();

    prompt.get(['pageName'], function (err, result) {
      if (err) {
        reject(err);
      }
      console.log('Command-line input received:');
      console.log('  layer: ' + result.pageName);
      resolve(result.pageName);
    });
  });
}

async function fetchPageIdIfExists(pageName) {
  const query = await queryPage(pageName);
  console.log(`queryPage: ${pageName}`, query);
  if (query.records.length) {
    return query.records[0].page_id;
  }
  return -1;
}

async function createPage(pageName) {
  const query = await queryWritePage(pageName);
  console.log(`createPage: ${pageName}`, query);
  if (query.numberOfRecordsUpdated === 1) {
    return query.insertId;
  }
  throw new Error(`unable to create page record for: ${pageName}`);
}

async function checkDBforHash(filePath, computedHash, pageId, checkId) {
  // todo call serverless aurora and find out if hash is already in DB
  const hashExists = await doesHashExist(computedHash);

  // if not in database write a record in the download table
  if (!hashExists) {
    console.log('Hash is unique.  Processing new download.');
    const downloadRecordId = await constructDownloadRecord(pageId, checkId, computedHash);

    // remove leading './' in rawDir path
    const rep = rawDir.replace('./', '');

    const originalFileName = filePath.split(`${rep}/`)[1];
    const s3KeyName = `${downloadRecordId}-${originalFileName}`;

    const extension = path.extname(originalFileName);

    if (extension !== '.zip') {
      console.error('expected filename extension to be .zip');
      console.error('unsure of what to do.  exiting.');
      process.exit();
    }

    // save downloaded file to the raw bucket in s3 (don't gzip here)
    await putFileToS3(rawBucket, s3KeyName, filePath, 'application/zip', false);

    // then extract
    return extractZip(filePath);
  }

  // otherwise, file has already been processed.
  console.log('Hash exists in database.  File has already been processed.\n');

  doBasicCleanup();
}

async function constructDownloadRecord(pageId, checkId, computedHash) {
  const query = await queryCreateDownloadRecord(pageId, checkId, computedHash);
  console.log(query);
  if (!query || !query.insertId) {
    throw new Error('unexpected result from create download request');
  }
  console.log('new download record created');
  return query.insertId;
}

function doBasicCleanup() {
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
}

async function doesHashExist() {
  const query = await queryHash(computedHash);
  if (query.records.length) {
    return true;
  }
  return false;
}

function extractZip(filePath) {
  fs.createReadStream(filePath)
    .pipe(unzipper.Extract({ path: unzippedDir }))
    .on('error', err => {
      console.error(`Error unzipping file: ${filePath}`);
      console.error(err);
      process.exit();
    })
    .on('close', () => {
      console.log(`Finished unzipping file: ${filePath}`);
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
