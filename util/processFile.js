const fs = require('fs');
const chalk = require('chalk');
const prompt = require('prompt');
const gdal = require('gdal-next');
const { getGeoJsonFromGdalFeature } = require('./parseFeature');
const { StatContext } = require('./StatContext');
const { outputDir, unzippedDir } = require('./constants');
const { queryGeographicIdentifier, queryCreateProductRecord } = require('./queries');
const { promptGeoIdentifiers } = require('./prompts');

// fileName is the file name without the extension
// fileType supports 'shapefile' and 'geodatabase'
exports.processFile = async function (fileName, fileType, downloadId) {
  // TODO use downloadID
  console.log(`Found file: ${fileName}`);

  console.log({ fileName, fileType });
  console.log(
    'opening from ' + `${unzippedDir}/${fileName + (fileType === 'shapefile' ? '.shp' : '')}`,
  );

  const dataset = gdal.open(
    `${unzippedDir}/${fileName + (fileType === 'shapefile' ? '.shp' : '')}`,
  );

  const driver = dataset.driver;
  const driver_metadata = driver.getMetadata();

  if (driver_metadata.DCAP_VECTOR !== 'YES') {
    console.error('Source file is not a vector');
    console.error('download record and s3 file are now orphaned.');
    console.log({ downloadId });
    process.exit(1);
  }

  console.log(`Driver = ${driver.description}\n`);

  let total_layers = 0; // iterator for layer labels

  // print out info for each layer in file
  dataset.layers.forEach(layer => {
    console.log(chalk.bold.red(`#${total_layers++}: ${layer.name}`));
    console.log(`  Geometry Type = ${gdal.Geometry.getName(layer.geomType)}`);
    console.log(chalk.dim(`  Spatial Reference = ${layer.srs ? layer.srs.toWKT() : 'null'}`));
    console.log('  Fields: ');
    layer.fields.forEach(field => {
      console.log(`    -${field.name} (${field.type})`);
    });
    console.log(chalk.blue(`  Feature Count = ${layer.features.count()}\n`));
  });

  let CHOSEN_TABLE = 0;

  if (total_layers !== 1) {
    // if just one layer, use it
    // otherwise, prompt for layer number

    try {
      const table_choice = await new Promise((resolve, reject) => {
        prompt.start();

        prompt.get(['layer'], function (err, result) {
          if (err) {
            reject(err);
          }
          console.log('Command-line input received:');
          console.log('  layer: ' + result.layer);
          resolve(result.layer);
        });
      });
      CHOSEN_TABLE = parseInt(table_choice);
    } catch (e) {
      console.error('ERROR getting user input');
      console.error(e);
      process.exit();
    }

    console.log({ CHOSEN_TABLE });
  }

  const SPLITTER = fileType === 'shapefile' ? '.shp' : '.gdb';

  const statCounter = new StatContext();

  const splitFileName = fileName.split(SPLITTER)[0];
  console.log(`processing file: ${fileName}`);

  let writeStream = fs.createWriteStream(`${outputDir}/${splitFileName}.ndgeojson`);

  // the finish event is emitted when all data has been flushed from the stream
  writeStream.on('finish', async () => {
    fs.writeFileSync(
      `${outputDir}/${splitFileName}.json`,
      JSON.stringify(statCounter.export()),
      'utf8',
    );
    console.log(
      `wrote all ${statCounter.rowCount} rows to file: ${outputDir}/${splitFileName}.ndgeojson\n`,
    );

    let fipsDetails;
    do {
      fipsDetails = await promptGeoIdentifiers();
    } while (!fipsDetails);
    console.log({ fipsDetails });

    // TODO get geo name corresponding to FIPS
    // this is up here so i can validate the geoid in the database before writing a product record
    // todo can probably move the figuring of geoid logic here
    // log to user the geoName so that the can have some assurance
    const { geoid, geoName } = await lookupCleanGeoName(fipsDetails);

    // This is probably done but hasnt been run
    const productId = await createProductRecord(fipsDetails, downloadId);

    // todo move these two functions out of here

    async function createProductRecord(geoid, downloadId) {
      const productType = 1; // original product (not filtered from a different product)

      const query = await queryCreateProductRecord(downloadId, productType, geoid);
      console.log(query);
      // todo something here
    }

    async function lookupCleanGeoName(fipsDetails) {
      const { SUMLEV, STATEFIPS, COUNTYFIPS, PLACEFIPS } = fipsDetails;

      let geoid;

      if (SUMLEV === '040') {
        geoid = STATEFIPS;
      } else if (SUMLEV === '050') {
        geoid = `${STATEFIPS}${COUNTYFIPS}`;
      } else if (SUMLEV === '160') {
        geoid = `${STATEFIPS}${PLACEFIPS}`;
      } else {
        console.error('SUMLEV out of range.  Exiting.');
        process.exit();
      }

      const query = queryGeographicIdentifier();
      console.log('remove this: geographic identifier query results:');
      console.log(query);
      // todo query for identifier
      // TODO Alter geo name to be s3 key friendly (all non alphanumeric become -)

      return { geoid, geoName };
    }

    // TODO upload ndgeojson and state files to S3 using downloadId and productId (concurrent)
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
  console.log(`found ${errored} feature errors`);
};
