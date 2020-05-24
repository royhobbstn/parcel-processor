const fs = require('fs');
const gdal = require('gdal-next');
const { getGeoJsonFromGdalFeature } = require('./parseFeature');
const { StatContext } = require('./StatContext');
const { outputDir, unzippedDir } = require('./constants');

// fileName is the file name without the extension
// fileType supports 'shapefile' and 'geodatabase'
exports.processFile = function (fileName, fileType) {
  console.log(`Found file: ${fileName}`);

  console.log({ fileName, fileType });
  console.log('opening from ' + `${unzippedDir}/${fileName}`);

  const dataset = gdal.open(`${unzippedDir}/${fileName + fileType === 'shapefile' ? '.shp' : ''}`);

  const driver = dataset.driver;
  const driver_metadata = driver.getMetadata();

  if (driver_metadata.DCAP_VECTOR !== 'YES') {
    console.error('Source file is not a vector');
    process.exit(1);
  }

  console.log(`Driver = ${driver.description}\n`);

  let i = 0; // iterator for layer labels

  // print out info for each layer in file
  dataset.layers.forEach(layer => {
    console.log(`${i++}: ${layer.name}`);
    console.log(`  Geometry Type = ${gdal.Geometry.getName(layer.geomType)}`);
    console.log(`  Spatial Reference = ${layer.srs ? layer.srs.toWKT() : 'null'}`);
    console.log('  Fields: ');
    layer.fields.forEach(field => {
      console.log(`    -${field.name} (${field.type})`);
    });
    console.log(`  Feature Count = ${layer.features.count()}\n`);
  });

  // if just one layer, use it
  // otherwise, prompt for layer number

  // TODO hardcoding for now
  const CHOSEN_TABLE = fileType === 'shapefile' ? 0 : 1;
  const SPLITTER = fileType === 'shapefile' ? '.shp' : '.gdb';

  const statCounter = new StatContext();

  const splitFileName = fileName.split(SPLITTER)[0];
  console.log(`processing file: ${fileName}`);

  let writeStream = fs.createWriteStream(`${outputDir}/${splitFileName}.ndgeojson`);

  // the finish event is emitted when all data has been flushed from the stream
  writeStream.on('finish', () => {
    fs.writeFileSync(
      `${outputDir}/${splitFileName}.json`,
      JSON.stringify(statCounter.export()),
      'utf8',
    );
    console.log(
      `wrote all ${statCounter.rowCount} rows to file: ${outputDir}/${splitFileName}.ndgeojson\n`,
    );
  });

  try {
    // setup coordinate projections
    var inputSpatialRef = dataset.layers.get(CHOSEN_TABLE).srs;
    var outputSpatialRef = gdal.SpatialReference.fromEPSG(4326);
    var transform = new gdal.CoordinateTransformation(inputSpatialRef, outputSpatialRef);
    var layer = dataset.layers.get(CHOSEN_TABLE);
  } catch (e) {
    console.error('Unknown problem reading table');
    console.error(gdal.lastError);
    console.error(e);
    process.exit();
  }

  // load features
  var layerFeatures = layer.features;
  var feat = null;
  let transformed = 0;
  let errored = 0;

  // transform features
  // this weird loop is because reading individual features can error
  // we want to ignore those errors and continue to write valid features
  let cont = true;
  do {
    try {
      feat = layerFeatures.next();
      if (!feat) {
        cont = false;
        writeStream.end();
      } else {
        const parsedFeature = getGeoJsonFromGdalFeature(feat, transform);
        statCounter.countStats(parsedFeature);
        writeStream.write(JSON.stringify(parsedFeature) + '\n', 'utf8');
        transformed++;
        if (transformed % 10000 === 0) {
          console.log(transformed + ' records processed');
        }
      }
    } catch (e) {
      writeStream.end();
      console.error(e);
      console.error(feat);
      console.error('Feature was ignored.');
      errored++;
    }
  } while (cont);

  console.log(`processed ${transformed} features`);
  console.log(`found ${errored} errors`);
};