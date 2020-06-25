// @ts-check

const fs = require('fs');
const chalk = require('chalk');
const spawn = require('child_process').spawn;
const gdal = require('gdal-next');
const ndjson = require('ndjson');
var turf = require('@turf/turf');
const { StatContext } = require('./StatContext');
const { directories, tileInfoPrefix, idPrefix, clusterPrefix } = require('./constants');
const { clustersKmeans } = require('./modKmeans');
const { log } = require('./logger');

exports.inspectFile = function (fileName, fileType) {
  log.info(`Found file: ${fileName}`);

  log.info({ fileName, fileType });
  log.info(
    'opening from ' +
      `${directories.unzippedDir}/${fileName + (fileType === 'shapefile' ? '.shp' : '')}`,
  );

  const dataset = gdal.open(
    `${directories.unzippedDir}/${fileName + (fileType === 'shapefile' ? '.shp' : '')}`,
  );

  const driver = dataset.driver;
  const driver_metadata = driver.getMetadata();

  if (driver_metadata.DCAP_VECTOR !== 'YES') {
    log.error('Source file is not a valid vector file.');
    throw new Error('Source file is not a valid vector file');
  }

  log.info(`Driver = ${driver.description}\n`);

  let total_layers = 0; // iterator for layer labels

  // print out info for each layer in file
  const layerSelection = dataset.layers
    .map((layer, index) => {
      console.log(chalk.bold.cyan(`#${total_layers++}: ${layer.name}`));
      console.log(`  Geometry Type = ${gdal.Geometry.getName(layer.geomType)}`);
      console.log(chalk.dim(`  Spatial Reference = ${layer.srs ? layer.srs.toWKT() : 'null'}`));
      console.log('  Fields: ');
      layer.fields.forEach(field => {
        console.log(`    -${field.name} (${field.type})`);
      });
      console.log(chalk.blue(`  Feature Count = ${layer.features.count()}\n`));
      return {
        index,
        type: gdal.Geometry.getName(layer.geomType),
        name: layer.name,
        count: layer.features.count(),
      };
    })
    .filter(d => {
      return d.type === 'Multi Polygon' || d.type === 'Polygon';
    })
    .sort((a, b) => {
      return b.count - a.count;
    });

  if (layerSelection.length === 0) {
    throw new Error('Could not find a suitable layer');
  }

  log.info('chosen layer: ', layerSelection[0]);

  const chosenLayer = layerSelection[0].index;

  return [dataset, chosenLayer];
};

exports.parseFile = function (dataset, chosenLayer, fileName, outputPath) {
  return new Promise((resolve, reject) => {
    try {
      // setup coordinate projections
      var inputSpatialRef = dataset.layers.get(chosenLayer).srs;
      var outputSpatialRef = gdal.SpatialReference.fromEPSG(4326);
      var transform = new gdal.CoordinateTransformation(inputSpatialRef, outputSpatialRef);
      var layer = dataset.layers.get(chosenLayer);
    } catch (e) {
      log.error('Unknown problem reading table');
      log.error(gdal.lastError);
      log.error(e);
      return reject(e);
    }

    log.info(`processing file: ${fileName}`);

    const statCounter = new StatContext();
    let writeStream = fs.createWriteStream(`${outputPath}.ndgeojson`);
    let counter = 0; // assign as parcel-outlet unique id
    let errored = 0;
    const points = []; // point centroid geojson features with parcel-outlet ids
    let propertyCount = 1; // will be updated below.  count of property attributes per feature.  used for deciding attribute file size and number of clusters

    // the finish event is emitted when all data has been flushed from the stream
    writeStream.on('finish', async () => {
      fs.writeFileSync(`${outputPath}.json`, JSON.stringify(statCounter.export()), 'utf8');
      log.info(`wrote all ${statCounter.rowCount} rows to file: ${outputPath}.ndgeojson\n`);

      log.info(`processed ${counter} features`);
      log.info(`found ${errored} feature errors\n`);
      return resolve([points, propertyCount]);
    });

    // load features
    var layerFeatures = layer.features;
    var feat = null;

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
          if (counter % 10000 === 0) {
            console.log(counter + ' records processed');
            // my as well do this here (it will happen on feature 0 at the least)
            propertyCount = Object.keys(parsedFeature.properties).length;
          }
          counter++;
          parsedFeature.properties[idPrefix] = counter;
          statCounter.countStats(parsedFeature);
          points.push(createPointFeature(parsedFeature));
          writeStream.write(JSON.stringify(parsedFeature) + '\n', 'utf8');
        }
      } catch (e) {
        log.error(e);
        log.error(feat);
        log.error('Feature was ignored.');
        errored++;
      }
    } while (cont);
  });
};

exports.addClusterIdToGeoData = function (points, propertyCount) {
  // write cluster-*(.ndgeojson) with a cluster id (to be used for making tiles)

  const featureCount = points.length;
  const numberOfClusters = Math.ceil((featureCount * propertyCount) / 30000);
  const clustered = clustersKmeans(turf.featureCollection(points), { numberOfClusters });

  // create a master lookup of __po_id to __po_cl  (idPrefix to clusterPrefix)
  const lookup = {};

  clustered.features.forEach((feature, idx) => {
    lookup[feature.properties[idPrefix]] = feature.properties.cluster;
  });

  return lookup;
};

function createPointFeature(parsedFeature) {
  const center = turf.center(parsedFeature);
  center.properties[idPrefix] = parsedFeature.properties[idPrefix];
  return center;
}

exports.parseOutputPath = function (fileName, fileType) {
  const SPLITTER = fileType === 'shapefile' ? '.shp' : '.gdb';
  const splitFileName = fileName.split(SPLITTER)[0];
  const outputPath = `${directories.outputDir}/${splitFileName}`;
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
    log.error('Unable to .getGeometry() from feature');
    log.error(e);
  }

  var clone;
  if (geometry) {
    try {
      clone = geometry.clone();
    } catch (e) {
      log.error('Unable to .clone() geometry');
      log.error(e);
    }
  }

  if (geometry && clone) {
    try {
      clone.transform(coordTransform);
    } catch (e) {
      log.error('Unable to .transform() geometry');
      log.error(e);
    }
  }

  var obj;
  if (geometry && clone) {
    try {
      clone.swapXY(); // ugh, you would think .toObject would take care of this
      obj = clone.toObject();
    } catch (e) {
      log.error('Unable to convert geometry .toObject()');
      log.error(e);
    }
  }

  geoJsonFeature.geometry = obj || [];
  geoJsonFeature.properties = feature.fields.toObject();
  return geoJsonFeature;
}

exports.convertToFormat = function (format, outputPath) {
  // convert from ndgeojson into geojson, gpkg, shp, etc

  return new Promise((resolve, reject) => {
    const application = 'ogr2ogr';
    const args = [
      '-f',
      format.driver,
      `${outputPath}.${format.extension}`,
      `${outputPath}.ndgeojson`,
    ];
    const command = `${application} ${args.join(' ')}`;
    log.info(`running: ${command}`);

    const proc = spawn(application, args);

    proc.stdout.on('data', data => {
      log.info(`stdout: ${data.toString()}`);
    });

    proc.stderr.on('data', data => {
      log.info(data.toString());
    });

    proc.on('error', err => {
      log.error(err);
      reject(err);
    });

    proc.on('close', code => {
      log.info(`completed creating format: ${format.driver}.`);
      resolve({ command });
    });
  });
};

exports.spawnTippecane = function (tilesDir, derivativePath) {
  return new Promise((resolve, reject) => {
    const layername = 'parcelslayer';
    const application = 'tippecanoe';
    const args = [
      '-f',
      '-l',
      layername,
      '-e',
      tilesDir,
      `--include=${idPrefix}`,
      `--include=${clusterPrefix}`,
      '-zg',
      '-pS',
      '-D10',
      '-M',
      '500000',
      '--coalesce-densest-as-needed',
      '--extend-zooms-if-still-dropping',
      `${derivativePath}.ndgeojson`,
    ];
    const command = `${application} ${args.join(' ')}`;
    log.info(`running: ${command}`);

    const proc = spawn(application, args);

    proc.stdout.on('data', data => {
      log.info(`stdout: ${data.toString()}`);
    });

    proc.stderr.on('data', data => {
      log.info(data.toString());
    });

    proc.on('error', err => {
      log.error(err);
      reject(err);
    });

    proc.on('close', code => {
      log.info(`completed creating tiles. code ${code}`);
      resolve({ command, layername });
    });
  });
};

exports.writeTileAttributes = function (derivativePath, tilesDir) {
  return new Promise((resolve, reject) => {
    log.info('processing attributes...');

    // make attributes directory
    fs.mkdirSync(`${tilesDir}/attributes`);

    let transformed = 0;

    fs.createReadStream(`${derivativePath}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', function (obj) {
        const copy = { ...obj.properties };
        delete copy[clusterPrefix]; // filter out clusterID property in stringified JSON
        fs.appendFileSync(
          `${tilesDir}/attributes/${tileInfoPrefix}cl_${obj.properties[clusterPrefix]}.ndjson`,
          JSON.stringify(copy) + '\n',
          'utf8',
        );

        transformed++;
        if (transformed % 10000 === 0) {
          log.info(transformed + ' records processed');
        }
      })
      .on('error', err => {
        log.error(err);
        reject('error');
      })
      .on('end', end => {
        log.info(transformed + ' records processed');
        resolve('end');
      });
  });
};

exports.createNdGeoJsonWithClusterId = async function (outputPath, lookup) {
  return new Promise((resolve, reject) => {
    log.info('creating derivative ndgeojson with clusterId...');

    let transformed = 0;
    log.info('createNdGeoJsonWithClusterId');
    log.info({ outputPath });
    const derivativePath = `${outputPath}-cluster`;

    fs.createReadStream(`${outputPath}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', function (obj) {
        // add clusterId
        obj.properties = { ...obj.properties, [clusterPrefix]: lookup[obj.properties[idPrefix]] };

        fs.appendFileSync(`${derivativePath}.ndgeojson`, JSON.stringify(obj) + '\n', 'utf8');

        transformed++;
        if (transformed % 10000 === 0) {
          log.info(transformed + ' records processed');
        }
      })
      .on('error', err => {
        log.error(err);
        reject('error');
      })
      .on('end', end => {
        log.info(transformed + ' records processed');
        resolve(derivativePath);
      });
  });
};
