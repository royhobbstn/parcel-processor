// @ts-check

const chokidar = require('chokidar');
const {
  directories,
  referenceIdLength,
  buckets,
  s3deleteType,
  modes,
} = require('./util/constants');
const {
  sourceInputPrompt,
  promptGeoIdentifiers,
  chooseGeoLayer,
  sourceTypePrompt,
  execSummaryPrompt,
  modePrompt,
} = require('./util/prompts');
const {
  fetchSourceIdIfExists,
  createSource,
  recordSourceCheck,
  doesHashExist,
  constructDownloadRecord,
  lookupCleanGeoName,
  checkHealth,
} = require('./util/wrappers/wrapQuery');
const {
  uploadRawFileToS3,
  createRawDownloadKey,
  removeS3Files,
} = require('./util/wrappers/wrapS3');
const { computeHash, generateRef } = require('./util/crypto');
const { doBasicCleanup } = require('./util/cleanup');
const { extractZip, checkForFileType } = require('./util/filesystemUtil');
const { inspectFile, parseOutputPath, parseFile } = require('./util/processGeoFile');
const { releaseProducts, createTiles } = require('./util/releaseProducts');
const {
  getConnection,
  startTransaction,
  commitTransaction,
  rollbackTransaction,
} = require('./util/primitives/queries');

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
      const connection = await getConnection();
      await checkHealth(connection);
      await startTransaction(connection);

      await doBasicCleanup([directories.outputDir, directories.unzippedDir], true);

      runMain(connection, executiveSummary, cleanupS3, filePath, mode)
        .then(async () => {
          console.log(`\n\nExecutive Summary\n`);
          console.log(executiveSummary.join('\n') + '\n');

          if (mode.label === modes.PRODUCTION.label) {
            // prompt with summary - throw if they say no
            await execSummaryPrompt();
            await commitTransaction(connection);
            await doBasicCleanup([
              directories.rawDir,
              directories.outputDir,
              directories.unzippedDir,
            ]);
          } else {
            await rollbackTransaction(connection);
            console.log(
              `\nBecause this was a ${mode.label} the database transactions have been rolled back.  \nYour file remains in the raw directory.\n`,
            );
            await doBasicCleanup([directories.outputDir, directories.unzippedDir]);
          }

          console.log('All transactions completed successfully.');
        })
        .catch(async err => {
          console.error(err);

          try {
            await rollbackTransaction(connection);
            console.error('All database transactions have been rolled back.');
          } catch (e) {
            console.error(e);
            console.error('Unable to rollback database!  Uh oh.');
          }

          if (mode.label === modes.PRODUCTION.label) {
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
          await connection.end();
          currentlyProcessing = false;
          console.log('\n\nawaiting a new file...\n');
        });
    });

  console.log('\nlistening...\n');
}

async function runMain(connection, executiveSummary, cleanupS3, filePath, mode) {
  const sourceNameInput = await sourceInputPrompt();

  let [sourceId, sourceType] = await fetchSourceIdIfExists(connection, sourceNameInput);

  if (sourceId === -1) {
    console.log(`Source doesn't exist in database.  Creating a new source.`);
    sourceType = await sourceTypePrompt(sourceNameInput);
    sourceId = await createSource(connection, sourceNameInput, sourceType);
    executiveSummary.push(
      `Created a new source record for: ${sourceNameInput} of type ${sourceType}`,
    );
  } else {
    executiveSummary.push(`Found source record for: ${sourceNameInput}`);
  }

  // todo, accomodate 'received' disposition by prompt
  const checkId = await recordSourceCheck(connection, sourceId, sourceType);
  executiveSummary.push(`Recorded source check as disposition: 'viewed'`);

  const computedHash = await computeHash(filePath);

  // call database and find out if hash is already in DB
  const hashExists = await doesHashExist(connection, computedHash);

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
  const { geoid, geoName } = await lookupCleanGeoName(connection, fipsDetails);
  executiveSummary.push(`geoName:  ${geoName},  geoid: ${geoid}`);

  const downloadRef = generateRef(referenceIdLength);

  // Contains ZIP extension.  create the key (path) to be used to store the zipfile in S3
  const rawKey = createRawDownloadKey(fipsDetails, geoid, geoName, downloadRef);

  // if not in database write a record in the download table
  const downloadId = await constructDownloadRecord(
    connection,
    sourceId,
    checkId,
    computedHash,
    rawKey,
    downloadRef,
    filePath,
  );
  executiveSummary.push(`created record in 'downloads' table.  ref: ${downloadRef}`);

  if (mode.label === modes.PRODUCTION.label) {
    await uploadRawFileToS3(filePath, rawKey);
    cleanupS3.push({ bucket: buckets.rawBucket, key: rawKey, type: s3deleteType.FILE });
    executiveSummary.push(`uploaded raw file to S3.  key: ${rawKey}`);
  }

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
  const [points, propertyCount] = await parseFile(dataset, chosenLayer, fileName, outputPath);

  const productKeys = await releaseProducts(
    connection,
    fipsDetails,
    geoid,
    geoName,
    downloadRef,
    downloadId,
    outputPath,
    executiveSummary,
    cleanupS3,
    mode,
  );

  // construct tiles (states dont get tiles.  too big.)
  if (fipsDetails.SUMLEV !== '040') {
    const productRefTiles = generateRef(referenceIdLength);
    const meta = {
      filePath,
      sourceId,
      geoid,
      geoName,
      fipsDetails,
      downloadId,
      downloadRef,
      outputPath,
      productRefTiles,
      rawKey,
      productKeys,
    };
    await createTiles(connection, meta, executiveSummary, cleanupS3, points, propertyCount, mode);
  } else {
    executiveSummary.push(`TILES generation doesn't run on States, and was skipped`);
  }
}
