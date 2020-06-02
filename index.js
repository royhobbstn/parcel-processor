const chokidar = require('chokidar');
const { rawDir, outputDir } = require('./util/constants');
const { acquireConnection } = require('./util/acquireConnection');
const { pageInputPrompt, promptGeoIdentifiers, chooseGeoLayer } = require('./util/prompts');
const {
  fetchPageIdIfExists,
  createPage,
  recordPageCheck,
  doesHashExist,
  constructDownloadRecord,
  createProductRecord,
  lookupCleanGeoName,
} = require('./util/wrappers/wrapQuery');
const { uploadRawFileToS3, uploadProductFiles } = require('./util/wrappers/wrapS3');
const { computeHash } = require('./util/crypto');
const { doBasicCleanup } = require('./util/cleanup');
const { extractZip, checkForFileType } = require('./util/filesystemUtil');
const { inspectFile, parseOutputPath, parseFile } = require('./util/processGeoFile');

init().catch(err => {
  console.error('unexpected error');
  console.error(err);
});

async function init() {
  await acquireConnection();
  watchFilesystem();
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

      const pageNameInput = await pageInputPrompt();

      let pageId = await fetchPageIdIfExists(pageNameInput);

      if (pageId === -1) {
        pageId = await createPage(pageNameInput);
      }

      const checkId = await recordPageCheck(pageId);

      const computedHash = await computeHash(filePath);

      // call database and find out if hash is already in DB
      const hashExists = await doesHashExist(computedHash);

      if (hashExists) {
        return doBasicCleanup();
      }

      // if not in database write a record in the download table
      const downloadId = await constructDownloadRecord(pageId, checkId, computedHash);

      await uploadRawFileToS3(filePath, downloadId);

      await extractZip(filePath);

      // determines if file(s) are of type shapefile or geodatabase
      const [fileName, fileType] = await checkForFileType(downloadId);

      // get SUMLEV, STATEFIPS, COUNTYFIPS, PLACEFIPS
      const fipsDetails = await promptGeoIdentifiers();

      // get geoname corresponding to FIPS
      const { geoid, geoName } = await lookupCleanGeoName(fipsDetails);

      // open file with OGR/GDAL
      const [dataset, total_layers] = inspectFile(fileName, fileType, downloadId);

      // choose layer to operate on (mostly for geodatabase)
      const chosenLayer = await chooseGeoLayer(total_layers);

      const outputPath = parseOutputPath(fileName, fileType, outputDir);

      // process all features and convert them to WGS84 ndgeojson
      // while gathering stats on the data
      await parseFile(dataset, chosenLayer, fileType, fileName, outputPath);

      const productId = await createProductRecord(geoid, downloadId);

      // upload ndgeojson and stat files to S3 (concurrent)
      await uploadProductFiles(downloadId, productId, geoName, geoid, fipsDetails, outputPath);

      console.log('cleaning up old files.\n');
      // todo cleanup

      console.log('done.\n');

      console.log('awaiting a new file...\n');
    });
  console.log('listening...\n');
}
