const chokidar = require('chokidar');
const { rawDir, outputDir, unzippedDir, referenceIdLength } = require('./util/constants');
const { acquireConnection } = require('./util/acquireConnection');
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
} = require('./util/wrappers/wrapQuery');
const { uploadRawFileToS3, createRawDownloadKey } = require('./util/wrappers/wrapS3');
const { computeHash, generateRef } = require('./util/crypto');
const { doBasicCleanup } = require('./util/cleanup');
const { extractZip, checkForFileType } = require('./util/filesystemUtil');
const { inspectFile, parseOutputPath, parseFile } = require('./util/processGeoFile');
const { releaseProducts } = require('./util/releaseProducts');

init().catch(err => {
  console.error('unexpected error');
  console.error(err);
});

// watch filesystem and compute hash of incoming file.
async function init() {
  chokidar
    .watch(rawDir, {
      ignored: /(^|[\/\\])\../, // ignore dotfiles
      awaitWriteFinish: true,
    })
    .on('add', async filePath => {
      console.log(`\nFile found: ${filePath}`);

      await acquireConnection();

      await doBasicCleanup([outputDir, unzippedDir]);

      const sourceNameInput = await sourceInputPrompt();

      const sourceType = await sourceTypePrompt(sourceNameInput);

      let sourceId = await fetchSourceIdIfExists(sourceNameInput);

      if (sourceId === -1) {
        console.log(`Source doesn't exist in database.  Creating a new source.`);
        sourceId = await createSource(sourceNameInput, sourceType);
      }

      console.log({ sourceType });

      const checkId = await recordSourceCheck(sourceId, sourceType);

      const computedHash = await computeHash(filePath);

      // call database and find out if hash is already in DB
      const hashExists = await doesHashExist(computedHash);

      if (hashExists) {
        return doBasicCleanup([rawDir]);
      }

      // get SUMLEV, STATEFIPS, COUNTYFIPS, PLACEFIPS
      const fipsDetails = await promptGeoIdentifiers();

      // get geoname corresponding to FIPS
      const { geoid, geoName } = await lookupCleanGeoName(fipsDetails);

      const downloadRef = generateRef(referenceIdLength);

      // Contains ZIP extension.  create the key (path) to be used to store the zipfile in S3
      const rawKey = createRawDownloadKey(fipsDetails, geoid, geoName, downloadRef);

      // if not in database write a record in the download table
      const downloadId = await constructDownloadRecord(
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
      const outputPath = parseOutputPath(fileName, fileType, outputDir);

      // process all features and convert them to WGS84 ndgeojson
      // while gathering stats on the data.  Writes ndgeojson and stat files to output.
      await parseFile(dataset, chosenLayer, fileName, outputPath);

      await releaseProducts(fipsDetails, geoid, geoName, downloadRef, downloadId, outputPath);

      // todo construct tiles

      // await doBasicCleanup([rawDir, outputDir, unzippedDir]);

      console.log('\nawaiting a new file...\n');
    });

  console.log('\nlistening...\n');
}
