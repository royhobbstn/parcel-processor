// @ts-check

const chokidar = require('chokidar');
const { directories, referenceIdLength, modes } = require('./util/constants');
const {
  sourceInputPrompt,
  promptGeoIdentifiers,
  chooseGeoLayer,
  execSummaryPrompt,
  modePrompt,
} = require('./util/prompts');
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
const { rollbackTransaction, commitTransaction } = require('./util/primitives/queries');

init().catch(err => {
  console.error('unexpected error');
  console.error(err);
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
        console.log(`currently processing an existing file. ${filePath} will be ignored.`);
        return;
      }

      currentlyProcessing = true;
      const cleanupS3 = [];
      const executiveSummary = [];

      console.log(`\nFile found: ${filePath}`);

      const mode = await modePrompt();

      await doBasicCleanup([directories.outputDir, directories.unzippedDir], true);

      let persistTransactionId = undefined;

      runMain(executiveSummary, cleanupS3, filePath, mode)
        .then(async transactionId => {
          persistTransactionId = transactionId;

          console.log(`\n\nExecutive Summary\n`);
          console.log(executiveSummary.join('\n') + '\n');

          if (mode.label === modes.FULL_RUN.label) {
            // prompt with summary - throw if they say no
            await execSummaryPrompt();
            await acquireConnection();
            await commitTransaction(persistTransactionId);
            console.log('Database records have been committed.');
            await doBasicCleanup([
              directories.rawDir,
              directories.outputDir,
              directories.unzippedDir,
            ]);
          } else {
            console.log(
              `\nBecause this was a ${mode.label} no database records or S3 files have been written.  \nYour file remains in the raw directory.\n`,
            );
            await doBasicCleanup([directories.outputDir, directories.unzippedDir]);
          }

          console.log('Completed successfully.');
        })
        .catch(async err => {
          console.error(err);

          if (!persistTransactionId) {
            console.log('It appears you rejected a DRY-RUN.  No need, nothing was written.');
          }

          if (mode.label === modes.FULL_RUN.label) {
            await acquireConnection();
            await rollbackTransaction(persistTransactionId);
            console.error('Database transaction has been rolled back');
            try {
              await removeS3Files(cleanupS3);
              console.error('All created S3 assets have been deleted');
            } catch (e) {
              console.error(e);
              console.error('Unable to delete S3 files.');
            }
          }

          await doBasicCleanup([directories.outputDir, directories.unzippedDir], true);
          console.log('output and unzipped directories cleaned.  Original raw file left in place.');
        })
        .finally(async () => {
          currentlyProcessing = false;
          console.log('\n\nDone.  Exiting.\n');
          process.exit();
        });
    });

  console.log('\nlistening...\n');
}

async function runMain(executiveSummary, cleanupS3, filePath, mode) {
  const sourceNameInput = await sourceInputPrompt();

  const computedHash = await computeHash(filePath);

  // call database and find out if hash is already in DB
  await acquireConnection();
  const hashExists = await doesHashExist(computedHash);

  if (hashExists) {
    executiveSummary.push(`Hash for ${filePath} already exists.`);
    return doBasicCleanup([directories.rawDir]);
  }

  // get SUMLEV, STATEFIPS, COUNTYFIPS, PLACEFIPS
  const fipsDetails = await promptGeoIdentifiers();
  executiveSummary.push(
    `Identifier prompt;\n  SUMLEV: ${fipsDetails.SUMLEV}\n  STATEFIPS: ${fipsDetails.STATEFIPS}\n  COUNTYFIPS: ${fipsDetails.COUNTYFIPS}\n  PLACEFIPS: ${fipsDetails.PLACEFIPS}`,
  );

  // get geoname corresponding to FIPS
  const { geoid, geoName } = await lookupCleanGeoName(fipsDetails);
  executiveSummary.push(`geoName:  ${geoName},  geoid: ${geoid}`);

  const downloadRef = generateRef(referenceIdLength);

  // Contains ZIP extension.  create the key (path) to be used to store the zipfile in S3
  const rawKey = createRawDownloadKey(fipsDetails, geoid, geoName, downloadRef);

  await extractZip(filePath);

  // determines if file(s) are of type shapefile or geodatabase
  const [fileName, fileType] = await checkForFileType();

  // open file with OGR/GDAL
  const [dataset, total_layers] = inspectFile(fileName, fileType);

  // choose layer to operate on (mostly for geodatabase)
  const chosenLayer = await chooseGeoLayer(total_layers);

  // determine where on the local disk the output geo products will be written
  const outputPath = parseOutputPath(fileName, fileType);

  // process all features and convert them to WGS84 ndgeojson
  // while gathering stats on the data.  Writes ndgeojson and stat files to output.
  await parseFile(dataset, chosenLayer, fileName, outputPath);

  const productRef = generateRef(referenceIdLength);

  // contains no file extension.  Used as base key for -stat.json  and .ndgeojson
  const productKey = createProductDownloadKey(fipsDetails, geoid, geoName, downloadRef, productRef);

  if (mode.label === modes.FULL_RUN.label) {
    await S3Writes(cleanupS3, filePath, rawKey, productKey, outputPath, executiveSummary);

    const transactionId = await DBWrites(
      sourceNameInput,
      executiveSummary,
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
  //     executiveSummary,
  //     cleanupS3,
  //     points,
  //     propertyCount,
  //     mode,
  //   );
  // } else {
  //   executiveSummary.push(`TILES generation doesn't run on States, and was skipped`);
  // }
}
