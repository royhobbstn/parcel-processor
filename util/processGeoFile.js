const fs = require('fs');
const chalk = require('chalk');
const gdal = require('gdal-next');
const { StatContext } = require('./StatContext');
const { unzippedDir } = require('./constants');

exports.inspectFile = function (fileName, fileType, downloadId) {
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
    console.log(chalk.bold.cyan(`#${total_layers++}: ${layer.name}`));
    console.log(`  Geometry Type = ${gdal.Geometry.getName(layer.geomType)}`);
    console.log(chalk.dim(`  Spatial Reference = ${layer.srs ? layer.srs.toWKT() : 'null'}`));
    console.log('  Fields: ');
    layer.fields.forEach(field => {
      console.log(`    -${field.name} (${field.type})`);
    });
    console.log(chalk.blue(`  Feature Count = ${layer.features.count()}\n`));
  });

  return [dataset, total_layers];
};

exports.parseFile = function (dataset, chosenLayer, fileType, fileName, outputPath) {
  return new Promise((resolve, reject) => {
    try {
      // setup coordinate projections
      var inputSpatialRef = dataset.layers.get(chosenLayer).srs;
      var outputSpatialRef = gdal.SpatialReference.fromEPSG(4326);
      var transform = new gdal.CoordinateTransformation(inputSpatialRef, outputSpatialRef);
      var layer = dataset.layers.get(chosenLayer);
    } catch (e) {
      console.error('Unknown problem reading table');
      console.error(gdal.lastError);
      console.error(e);
      reject(e);
    }

    const statCounter = new StatContext();

    console.log(`processing file: ${fileName}`);

    let writeStream = fs.createWriteStream(`${outputPath}.ndgeojson`);

    // the finish event is emitted when all data has been flushed from the stream
    writeStream.on('finish', async () => {
      fs.writeFileSync(`${outputPath}.json`, JSON.stringify(statCounter.export()), 'utf8');
      console.log(`wrote all ${statCounter.rowCount} rows to file: ${outputPath}.ndgeojson\n`);

      console.log(`processed ${transformed} features`);
      console.log(`found ${errored} feature errors\n`);
      resolve();
    });

    // load features
    var layerFeatures = layer.features;
    var feat = null;
    let transformed = 0;
    let errored = 0;

    // transform features
    // this awkward loop is because reading individual features can error
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
  });
};

exports.parseOutputPath = function (fileName, fileType, outputDir) {
  const SPLITTER = fileType === 'shapefile' ? '.shp' : '.gdb';
  const splitFileName = fileName.split(SPLITTER)[0];
  const outputPath = `${outputDir}/${splitFileName}.json`;
  return outputPath;
};

function getGeoJsonFromGdalFeature(feature, coordTransform) {
  var geoJsonFeature = {
    type: 'Feature',
    properties: {},
  };

  var geometry;
  try {
    geometry = feature.getGeometry();
  } catch (e) {
    console.error('Unable to .getGeometry() from feature');
    console.error(e);
  }

  var clone;
  if (geometry) {
    try {
      clone = geometry.clone();
    } catch (e) {
      console.error('Unable to .clone() geometry');
      console.error(e);
    }
  }

  if (geometry && clone) {
    try {
      clone.transform(coordTransform);
    } catch (e) {
      console.error('Unable to .transform() geometry');
      console.error(e);
    }
  }

  var obj;
  if (geometry && clone) {
    try {
      obj = clone.toObject();
    } catch (e) {
      console.error('Unable to convert geometry .toObject()');
      console.error(e);
    }
  }

  geoJsonFeature.geometry = obj || [];
  geoJsonFeature.properties = feature.fields.toObject();
  return geoJsonFeature;
}
