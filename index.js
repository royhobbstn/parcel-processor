const chokidar = require('chokidar');
const { directories, referenceIdLength, buckets } = require('./util/constants');
const {
  sourceInputPrompt,
  promptGeoIdentifiers,
  chooseGeoLayer,
  sourceTypePrompt,
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
const { uploadRawFileToS3, createRawDownloadKey } = require('./util/wrappers/wrapS3');
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

      console.log(`\nFile found: ${filePath}`);

      const connection = await getConnection();
      await checkHealth(connection);
      await startTransaction(connection);

      runMain(connection, filePath)
        .then(async () => {
          await commitTransaction(connection);
        })
        .catch(async err => {
          await rollbackTransaction(connection);
        })
        .finally(async () => {
          await connection.end();
          currentlyProcessing = false;
        });
    });

  console.log('\nlistening...\n');
}

async function runMain(connection, filePath) {
  await doBasicCleanup([directories.outputDir, directories.unzippedDir]);

  const sourceNameInput = await sourceInputPrompt();

  const sourceType = await sourceTypePrompt(sourceNameInput);

  let sourceId = await fetchSourceIdIfExists(connection, sourceNameInput);

  if (sourceId === -1) {
    console.log(`Source doesn't exist in database.  Creating a new source.`);
    sourceId = await createSource(connection, sourceNameInput, sourceType);
  }

  console.log({ sourceType });

  const checkId = await recordSourceCheck(connection, sourceId, sourceType);

  const computedHash = await computeHash(filePath);

  // call database and find out if hash is already in DB
  const hashExists = await doesHashExist(connection, computedHash);

  if (hashExists) {
    return doBasicCleanup([directories.rawDir]);
  }

  // get SUMLEV, STATEFIPS, COUNTYFIPS, PLACEFIPS
  const fipsDetails = await promptGeoIdentifiers();

  // get geoname corresponding to FIPS
  const { geoid, geoName } = await lookupCleanGeoName(connection, fipsDetails);

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

  await uploadRawFileToS3(filePath, rawKey);

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

  const productKeys = await releaseProducts(
    connection,
    fipsDetails,
    geoid,
    geoName,
    downloadRef,
    downloadId,
    outputPath,
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
    await createTiles(connection, meta);
  }

  // await doBasicCleanup([directories.rawDir, directories.outputDir, directories.unzippedDir]);

  currentlyProcessing = false;
  console.log('\nawaiting a new file...\n');
}
