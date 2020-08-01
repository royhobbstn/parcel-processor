const { inspectFileExec, parseFileExec } = require('../util/processGeoFile.js');
const ctx = { log: console, process: [] };

async function main() {
  const fileName = 'Election.gdb';
  const fileType = 'geodatabase';

  // todo problem with geopackage as input here?
  const inputPath = `${directories.unzippedDir + ctx.directoryId}/${
    fileName + (fileType === 'shapefile' ? '.shp' : '')
  }`;

  const chosenLayerName = await inspectFileExec(ctx, inputPath);

  // determine where on the local disk the output geo products will be written
  const outputPath = parseOutputPath(ctx, fileName, fileType);

  // process all features and convert them to WGS84 ndgeojson
  // while gathering stats on the data.  Writes ndgeojson and stat files to output.
  console.log('about to parse');
  await parseFileExec(ctx, fileName, outputPath, inputPath, chosenLayerName);
}

main()
  .then(() => {
    console.log('done');
  })
  .catch(err => {
    console.error('error', { error: err.message, stack: err.stack });
  });
