// @ts-check

const fs = require('fs');
const spawn = require('child_process').spawn;
const gdal = require('gdal-next');
const ndjson = require('ndjson');
var turf = require('@turf/turf');
const { StatContext } = require('./StatContext');
const { directories, tileInfoPrefix, idPrefix, clusterPrefix } = require('./constants');
const { clustersKmeans } = require('./modKmeans');

exports.inspectFile = function (ctx, fileName, fileType) {
  ctx.log.info(`Found file: ${fileName}`);

  ctx.log.info({ fileName, fileType });
  ctx.log.info(
    'opening from ' +
      `${directories.unzippedDir}/${fileName + (fileType === 'shapefile' ? '.shp' : '')}`,
  );

  const dataset = gdal.open(
    `${directories.unzippedDir}/${fileName + (fileType === 'shapefile' ? '.shp' : '')}`,
  );

  const driver = dataset.driver;
  const driver_metadata = driver.getMetadata();

  if (driver_metadata.DCAP_VECTOR !== 'YES') {
    ctx.log.error('Source file is not a valid vector file.');
    throw new Error('Source file is not a valid vector file');
  }

  ctx.log.info(`Driver = ${driver.description}\n`);

  let total_layers = 0; // iterator for layer labels

  // print out info for each layer in file
  const layerSelection = dataset.layers
    .map((layer, index) => {
      ctx.log(`#${total_layers++}: ${layer.name}`);
      ctx.log(`  Geometry Type = ${gdal.Geometry.getName(layer.geomType)}`);
      ctx.log(`  Spatial Reference = ${layer.srs ? layer.srs.toWKT() : 'null'}`);
      ctx.log('  Fields: ');
      layer.fields.forEach(field => {
        ctx.log(`    -${field.name} (${field.type})`);
      });
      ctx.log(`  Feature Count = ${layer.features.count()}`);
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

  ctx.log.info('chosen layer: ', layerSelection[0]);

  const chosenLayer = layerSelection[0].index;

  return [dataset, chosenLayer];
};

exports.parseFile = function (ctx, dataset, chosenLayer, fileName, outputPath) {
  return new Promise((resolve, reject) => {
    let layer;
    let transform;
    try {
      // setup coordinate projections
      const inputSpatialRef = dataset.layers.get(chosenLayer).srs;
      const outputSpatialRef = gdal.SpatialReference.fromEPSG(4326);
      transform = new gdal.CoordinateTransformation(inputSpatialRef, outputSpatialRef);
      layer = dataset.layers.get(chosenLayer);
    } catch (err) {
      ctx.log.error('Unknown problem reading table', { err: err.message, stack: err.stack });
      ctx.log.error('Last GDAL Error: ', { last: gdal.lastError });
      return reject(err);
    }

    ctx.log.info(`processing file: ${fileName}`);

    const statCounter = new StatContext(ctx);
    let writeStream = fs.createWriteStream(`${outputPath}.ndgeojson`);
    let counter = 0; // assign as parcel-outlet unique id
    let errored = 0;

    // the finish event is emitted when all data has been flushed from the stream
    writeStream.on('finish', async () => {
      fs.writeFileSync(`${outputPath}.json`, JSON.stringify(statCounter.export()), 'utf8');
      ctx.log.info(`wrote all ${statCounter.rowCount} rows to file: ${outputPath}.ndgeojson\n`);

      ctx.log.info(`processed ${counter} features`);
      ctx.log.info(`found ${errored} feature errors\n`);
      return resolve();
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
          const parsedFeature = getGeoJsonFromGdalFeature(ctx, feat, transform);
          if (counter % 10000 === 0) {
            ctx.log.info(counter + ' records processed');
          }
          counter++;
          parsedFeature.properties[idPrefix] = counter;
          statCounter.countStats(parsedFeature);
          writeStream.write(JSON.stringify(parsedFeature) + '\n', 'utf8');
        }
      } catch (err) {
        ctx.log.error('Feature was ignored.', { err: err.message, stack: err.stack });
        errored++;
      }
    } while (cont);
  });
};

exports.addClusterIdToGeoData = function (ctx, points, propertyCount) {
  // write cluster-*(.ndgeojson) with a cluster id (to be used for making tiles)

  const featureCount = points.length;
  const numberOfClusters = Math.ceil((featureCount * propertyCount) / 30000);
  const clustered = clustersKmeans(ctx, turf.featureCollection(points), { numberOfClusters });

  // create a master lookup of __po_id to __po_cl  (idPrefix to clusterPrefix)
  const lookup = {};

  clustered.features.forEach((feature, idx) => {
    lookup[feature.properties[idPrefix]] = feature.properties.cluster;
  });

  return lookup;
};

function createPointFeature(ctx, parsedFeature) {
  const center = turf.center(parsedFeature);
  center.properties[idPrefix] = parsedFeature.properties[idPrefix];
  return center;
}

exports.parseOutputPath = function (ctx, fileName, fileType) {
  const SPLITTER = fileType === 'shapefile' ? '.shp' : '.gdb';
  const splitFileName = fileName.split(SPLITTER)[0];
  const outputPath = `${directories.outputDir}/${splitFileName}`;
  return outputPath;
};

function getGeoJsonFromGdalFeature(ctx, feature, coordTransform) {
  var geoJsonFeature = {
    type: 'Feature',
    properties: {},
  };

  var geometry;
  try {
    geometry = feature.getGeometry();
  } catch (err) {
    ctx.log.error('Unable to .getGeometry() from feature', { err: err.message, stack: err.stack });
  }

  var clone;
  if (geometry) {
    try {
      clone = geometry.clone();
    } catch (err) {
      ctx.log.error('Unable to .clone() geometry', { err: err.message, stack: err.stack });
    }
  }

  if (geometry && clone) {
    try {
      clone.transform(coordTransform);
    } catch (err) {
      ctx.log.error('Unable to .transform() geometry', { err: err.message, stack: err.stack });
    }
  }

  var obj;
  if (geometry && clone) {
    try {
      clone.swapXY(); // ugh, you would think .toObject would take care of this
      obj = clone.toObject();
    } catch (err) {
      ctx.log.error('Unable to convert geometry .toObject()', {
        err: err.message,
        stack: err.stack,
      });
    }
  }

  geoJsonFeature.geometry = obj || [];
  geoJsonFeature.properties = feature.fields.toObject();
  return geoJsonFeature;
}

exports.convertToFormat = function (ctx, format, outputPath) {
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
    ctx.log.info(`running: ${command}`);

    const proc = spawn(application, args);

    proc.stdout.on('data', data => {
      ctx.log.info(`stdout: ${data.toString()}`);
    });

    proc.stderr.on('data', data => {
      ctx.log.info(data.toString());
    });

    proc.on('error', err => {
      ctx.log.error('Error', { err: err.message, stack: err.stack });
      reject(err);
    });

    proc.on('close', code => {
      ctx.log.info(`completed creating format: ${format.driver}.`);
      resolve({ command });
    });
  });
};

exports.spawnTippecane = function (ctx, tilesDir, derivativePath) {
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
    ctx.log.info(`running: ${command}`);

    const proc = spawn(application, args);

    proc.stdout.on('data', data => {
      ctx.log.info(data.toString());
    });

    proc.stderr.on('data', data => {
      console.log(data.toString());
    });

    proc.on('error', err => {
      ctx.log.error('Error', { err: err.message, stack: err.stack });
      reject(err);
    });

    proc.on('close', code => {
      ctx.log.info(`completed creating tiles. code ${code}`);
      resolve({ command, layername });
    });
  });
};

exports.writeTileAttributes = function (ctx, derivativePath, tilesDir) {
  return new Promise((resolve, reject) => {
    ctx.log.info('processing attributes...');

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
          ctx.log.info(transformed + ' records processed');
        }
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        reject('error');
      })
      .on('end', end => {
        ctx.log.info(transformed + ' records processed');
        resolve('end');
      });
  });
};

exports.extractPointsFromNdGeoJson = async function (ctx, outputPath) {
  return new Promise((resolve, reject) => {
    ctx.log.info('creating derivative ndgeojson with clusterId...');

    let transformed = 0;
    ctx.log.info('extractPointsFromNdGeoJson');
    ctx.log.info({ outputPath });
    const points = []; // point centroid geojson features with parcel-outlet ids
    let propertyCount = 1; // will be updated below.  count of property attributes per feature.  used for deciding attribute file size and number of clusters

    fs.createReadStream(`${outputPath}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', function (obj) {
        // create a point feature
        points.push(createPointFeature(ctx, obj));

        transformed++;
        if (transformed % 10000 === 0) {
          // may as well do this here (it will happen on feature 0 at the least)
          propertyCount = Object.keys(obj.properties).length;
          ctx.log.info(transformed + ' records processed');
        }
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('end', end => {
        ctx.log.info(transformed + ' records processed');
        return resolve([points, propertyCount]);
      });
  });
};

exports.createNdGeoJsonWithClusterId = async function (ctx, outputPath, lookup) {
  return new Promise((resolve, reject) => {
    ctx.log.info('creating derivative ndgeojson with clusterId...');

    let transformed = 0;
    ctx.log.info('createNdGeoJsonWithClusterId');
    ctx.log.info({ outputPath });
    const derivativePath = `${outputPath}-cluster`;

    fs.createReadStream(`${outputPath}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', function (obj) {
        // add clusterId
        obj.properties = { ...obj.properties, [clusterPrefix]: lookup[obj.properties[idPrefix]] };

        fs.appendFileSync(`${derivativePath}.ndgeojson`, JSON.stringify(obj) + '\n', 'utf8');

        transformed++;
        if (transformed % 10000 === 0) {
          ctx.log.info(transformed + ' records processed');
        }
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        reject('error');
      })
      .on('end', end => {
        ctx.log.info(transformed + ' records processed');
        resolve(derivativePath);
      });
  });
};
