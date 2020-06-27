// @ts-check

const chokidar = require('chokidar');
const { directories, referenceIdLength, modes } = require('./util/constants');
const { sourceInputPrompt, promptGeoIdentifiers, modePrompt } = require('./util/prompts');
const {
  doesHashExist,
  lookupCleanGeoName,
  acquireConnection,
  DBWrites,
} = require('./util/wrappers/wrapQuery');
const {
  createRawDownloadKey,
  removeS3Files,
  createProductDownloadKey,
  S3Writes,
} = require('./util/wrappers/wrapS3');
const { computeHash, generateRef } = require('./util/crypto');
const { doBasicCleanup } = require('./util/cleanup');
const { extractZip, checkForFileType } = require('./util/filesystemUtil');
const { inspectFile, parseOutputPath, parseFile } = require('./util/processGeoFile');
const { log } = require('./util/logger');

init().catch(err => {
  log.error('unexpected error');
  log.error(err);
  process.exit();
});

// watch filesystem and compute hash of incoming file.
async function init() {
  let currentlyProcessing = false;

  chokidar
    .watch(directories.rawDir, {
      ignored: /(^|[\/\\])\../, // ignore dotfiles
      awaitWriteFinish: true,
    })
    .on('add', async filePath => {
      if (currentlyProcessing) {
        log.warn(`currently processing an existing file. ${filePath} will be ignored.`);
        return;
      }

      currentlyProcessing = true;
      const cleanupS3 = [];

      log.info(`\nFile found: ${filePath}`);

      const mode = await modePrompt();

      await doBasicCleanup([directories.outputDir, directories.unzippedDir], true, true);

      runMain(cleanupS3, filePath, mode)
        .then(async () => {
          if (mode.label === modes.FULL_RUN.label) {
            await doBasicCleanup([
              directories.rawDir,
              directories.outputDir,
              directories.unzippedDir,
            ]);
          } else {
            log.info(
              `\nBecause this was a ${mode.label} no database records or S3 files have been written.  \nYour file remains in the raw directory.\n`,
            );
            await doBasicCleanup([directories.outputDir, directories.unzippedDir]);
          }

          log.info('Completed successfully.');
        })
        .catch(async err => {
          log.error(err);

          if (mode.label === modes.FULL_RUN.label) {
            try {
              await removeS3Files(cleanupS3);
              log.error('All created S3 assets have been deleted');
            } catch (e) {
              log.error(e);
              log.error('Unable to delete S3 files.');
            }
          }

          await doBasicCleanup([directories.outputDir, directories.unzippedDir], true);
          log.info('output and unzipped directories cleaned.  Original raw file left in place.');
        })
        .finally(async () => {
          currentlyProcessing = false;
          log.info('\n\nDone. Ctrl-C to exit.\n');
        });
    });

  log.info('--------------------------------------------');
  log.info('listening...');
}

async function runMain(cleanupS3, filePath, mode) {
  const sourceNameInput = await sourceInputPrompt();

  const computedHash = await computeHash(filePath);

  // call database and find out if hash is already in DB
  await acquireConnection();
  const hashExists = await doesHashExist(computedHash);

  if (hashExists) {
    log.info(`Hash for ${filePath} already exists.`);
    // todo uncomment // return doBasicCleanup([directories.rawDir]);
  }

  // get SUMLEV, STATEFIPS, COUNTYFIPS, PLACEFIPS
  const fipsDetails = await promptGeoIdentifiers();
  log.info(
    `Identifier prompt;\n  SUMLEV: ${fipsDetails.SUMLEV}\n  STATEFIPS: ${fipsDetails.STATEFIPS}\n  COUNTYFIPS: ${fipsDetails.COUNTYFIPS}\n  PLACEFIPS: ${fipsDetails.PLACEFIPS}`,
  );

  // get geoname corresponding to FIPS
  const { geoid, geoName } = await lookupCleanGeoName(fipsDetails);
  log.info(`geoName:  ${geoName},  geoid: ${geoid}`);

  const downloadRef = generateRef(referenceIdLength);

  // Contains ZIP extension.  create the key (path) to be used to store the zipfile in S3
  const rawKey = createRawDownloadKey(fipsDetails, geoid, geoName, downloadRef);

  await extractZip(filePath);

  // determines if file(s) are of type shapefile or geodatabase
  const [fileName, fileType] = await checkForFileType();

  // open file with OGR/GDAL
  const [dataset, chosenLayer] = inspectFile(fileName, fileType);

  // determine where on the local disk the output geo products will be written
  const outputPath = parseOutputPath(fileName, fileType);

  // process all features and convert them to WGS84 ndgeojson
  // while gathering stats on the data.  Writes ndgeojson and stat files to output.
  await parseFile(dataset, chosenLayer, fileName, outputPath);

  const productRef = generateRef(referenceIdLength);
  const individualRef = generateRef(referenceIdLength);

  // contains no file extension.  Used as base key for -stat.json  and .ndgeojson
  const productKey = createProductDownloadKey(
    fipsDetails,
    geoid,
    geoName,
    downloadRef,
    productRef,
    individualRef,
  );

  if (mode.label === modes.FULL_RUN.label) {
    await S3Writes(cleanupS3, filePath, rawKey, productKey, outputPath);

    const transactionId = await DBWrites(
      sourceNameInput,
      computedHash,
      rawKey,
      downloadRef,
      filePath,
      productRef,
      geoid,
      productKey,
    );

    return transactionId;
  }

  return;

  // TODO points, propertyCount should be somewhere else

  // TODO split saving NDGeoJSON record back out here to main.

  // ------- end.  everything below is separate lambda

  // // construct tiles (states dont get tiles.  too big.)
  // if (fipsDetails.SUMLEV !== '040') {
  //   const productRefTiles = generateRef(referenceIdLength);
  //   const meta = {
  //     filePath,
  //     sourceId,
  //     geoid,
  //     geoName,
  //     fipsDetails,
  //     downloadId,
  //     downloadRef,
  //     outputPath,
  //     productRefTiles,
  //     rawKey,
  //   };
  //   await createTiles(
  //     meta,
  //     cleanupS3,
  //     points,
  //     propertyCount,
  //     mode,
  //   );
  // } else {
  //   console.log(`TILES generation doesn't run on States, and was skipped`);
  // }
}
