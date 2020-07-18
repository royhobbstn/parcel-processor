// @ts-check

const fs = require('fs');
const spawn = require('child_process').spawn;
const gdal = require('gdal-next');
const ndjson = require('ndjson');
var turf = require('@turf/turf');
const { StatContext } = require('./StatContext');
const {
  directories,
  tileInfoPrefix,
  idPrefix,
  clusterPrefix,
  fileFormats,
} = require('./constants');
const { clustersKmeans } = require('./modKmeans');
const { sleep, unwindStack } = require('./misc');

exports.inspectFile = function (ctx, fileName, fileType) {
  ctx.process.push('inspectFile');

  ctx.log.info(`Found file: ${fileName}`);

  ctx.log.info(
    'opening from ' +
      `${directories.unzippedDir + ctx.directoryId}/${
        fileName + (fileType === 'shapefile' ? '.shp' : '')
      }`,
  );

  const dataset = gdal.open(
    `${directories.unzippedDir + ctx.directoryId}/${
      fileName + (fileType === 'shapefile' ? '.shp' : '')
    }`,
  );

  const driver = dataset.driver;
  const driver_metadata = driver.getMetadata();

  if (driver_metadata.DCAP_VECTOR !== 'YES') {
    ctx.log.error('Source file is not a valid vector file.');
    throw new Error('Source file is not a valid vector file');
  }

  ctx.log.info(`Driver = ${driver.description}`);

  let total_layers = 0; // iterator for layer labels

  // print out info for each layer in file
  const layerSelection = dataset.layers
    .map((layer, index) => {
      ctx.log.info(`#${total_layers++}: ${layer.name}`);
      ctx.log.info(`  Geometry Type = ${gdal.Geometry.getName(layer.geomType)}`);
      ctx.log.info(
        `  Spatial Reference = ${
          layer.srs ? layer.srs.toWKT().replace(/\"/g, "'") : 'no spatial reference defined'
        }`,
      );
      ctx.log.info('  Fields: ');
      layer.fields.forEach(field => {
        ctx.log.info(`    -${field.name} (${field.type})`);
      });
      ctx.log.info(`  Feature Count = ${layer.features.count()}`);
      return {
        index,
        type: gdal.Geometry.getName(layer.geomType),
        name: layer.name,
        count: layer.features.count(),
      };
    })
    .filter(d => {
      // 3D Measured Multi Polygon?  Really, West Virginia?
      return [
        '3D Measured Polygon',
        '3D Polygon',
        'Measured Polygon',
        'Polygon',
        '3D Measured Multi Polygon',
        '3D Multi Polygon',
        'Measured Multi Polygon',
        'Multi Polygon',
      ].includes(d.type);
    })
    .sort((a, b) => {
      return b.count - a.count;
    });

  if (layerSelection.length === 0) {
    throw new Error('Could not find a suitable layer');
  }

  ctx.log.info('chosen layer: ', layerSelection[0]);

  const chosenLayer = layerSelection[0].index;

  ctx.process.pop();
  return [dataset, chosenLayer];
};

exports.parseFile = function (ctx, dataset, chosenLayer, fileName, outputPath) {
  ctx.process.push('parseFile');

  return new Promise(async (resolve, reject) => {
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
    const geometryErrors = {
      'Unable to .getGeometry() from feature': 0,
      'Unable to .clone() geometry': 0,
      'Unable to .transform() geometry': 0,
      'Unable to convert geometry .toObject()': 0,
    };
    // the finish event is emitted when all data has been flushed from the stream
    writeStream.on('finish', () => {
      fs.writeFileSync(`${outputPath}.json`, JSON.stringify(statCounter.export()), 'utf8');
      ctx.log.info(`wrote all ${statCounter.rowCount} rows to file: ${outputPath}.ndgeojson`);
      ctx.log.info(`processed ${counter} features`);
      ctx.log.info(`found ${errored} feature errors`);
      ctx.log.info(`geometry error breakdown: `, { geometryErrors });
      unwindStack(ctx.process, 'parseFile');
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
          const parsedFeature = getGeoJsonFromGdalFeature(ctx, feat, transform, geometryErrors);
          if (counter % 10000 === 0) {
            ctx.log.info(counter + ' records processed');
            await sleep(100);
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
  ctx.process.push('addClusterIdToGeoData');

  // write cluster-*(.ndgeojson) with a cluster id (to be used for making tiles)
  ctx.log.info('adding clusterId to GeoData using KMeans');

  const featureCount = points.length;
  const numberOfClusters = Math.ceil((featureCount * propertyCount) / 30000);
  const clustered = clustersKmeans(ctx, turf.featureCollection(points), { numberOfClusters });
  ctx.log.info('finished clustering');

  // create a master lookup of __po_id to __po_cl  (idPrefix to clusterPrefix)
  const lookup = {};

  clustered.features.forEach((feature, idx) => {
    if (idx % 10000 === 0) {
      ctx.log.info(`creating lookup, feature # ${idx}`);
    }
    lookup[feature.properties[idPrefix]] = feature.properties.cluster;
  });

  unwindStack(ctx.process, 'addClusterIdToGeoData');
  return lookup;
};

function createPointFeature(ctx, parsedFeature) {
  try {
    const center = turf.center(parsedFeature);
    center.properties[idPrefix] = parsedFeature.properties[idPrefix];
    return center;
  } catch (err) {
    ctx.log.info('ignoring error creating point feature in:', { __po_id: parsedFeature.__po_id });
    return null;
  }
}

exports.parseOutputPath = function (ctx, fileName, fileType) {
  const SPLITTER = fileType === 'shapefile' ? '.shp' : '.gdb';
  const splitFileName = fileName.split(SPLITTER)[0];
  const outputPath = `${directories.outputDir + ctx.directoryId}/${splitFileName}`;
  return outputPath;
};

function getGeoJsonFromGdalFeature(ctx, feature, coordTransform, geometryErrors) {
  var geoJsonFeature = {
    type: 'Feature',
    properties: {},
  };

  var geometry;
  try {
    geometry = feature.getGeometry();
  } catch (err) {
    geometryErrors['Unable to .getGeometry() from feature'] += 1;
  }

  var clone;
  if (geometry) {
    try {
      clone = geometry.clone();
    } catch (err) {
      geometryErrors['Unable to .clone() geometry'] += 1;
    }
  }

  if (geometry && clone) {
    try {
      clone.transform(coordTransform);
    } catch (err) {
      geometryErrors['Unable to .transform() geometry'] += 1;
    }
  }

  var obj;
  if (geometry && clone) {
    try {
      clone.swapXY(); // ugh, you would think .toObject would take care of this
      obj = clone.toObject();
    } catch (err) {
      geometryErrors['Unable to convert geometry .toObject()'] += 1;
    }
  }

  geoJsonFeature.geometry = obj || [];
  geoJsonFeature.properties = feature.fields.toObject();

  return geoJsonFeature;
}

exports.convertToFormat = function (ctx, format, outputPath) {
  ctx.process.push('convertToFormat');
  ctx.process.push(format.extension);

  // convert from ndgeojson into geojson, gpkg, shp, etc

  return new Promise((resolve, reject) => {
    const application = 'ogr2ogr';
    const args = [
      '-f',
      format.driver,
      `${outputPath}.${format.extension}`,
      `${outputPath}.ndgeojson`,
    ];

    // geojson needs RFC option specified
    if (format.extension === fileFormats.GEOJSON.extension) {
      args.push('-lco', 'RFC7946=YES');
    }

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
      return reject(err);
    });

    proc.on('close', code => {
      ctx.log.info(`completed creating format: ${format.driver}.`);
      ctx.process.pop();
      unwindStack(ctx.process, format.extension);
      unwindStack(ctx.process, 'convertToFormat');
      return resolve({ command });
    });
  });
};

exports.spawnTippecane = function (ctx, tilesDir, derivativePath) {
  ctx.process.push('spawnTippecane');

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
      '512000',
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
      unwindStack(ctx.process, 'spawnTippecane');
      resolve({ command, layername });
    });
  });
};

exports.writeTileAttributes = function (ctx, derivativePath, tilesDir) {
  ctx.process.push('writeTileAttributes');

  return new Promise((resolve, reject) => {
    ctx.log.info('processing attributes...');

    // make attributes directory
    fs.mkdirSync(`${tilesDir}/attributes`);

    let transformed = 0;

    const writeStreams = {};
    const writePromises = [];

    fs.createReadStream(`${derivativePath}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', async function (obj) {
        const copy = { ...obj.properties };
        delete copy[clusterPrefix]; // filter out clusterID property in stringified JSON
        const prefix = obj.properties[clusterPrefix];

        if (!writeStreams[prefix]) {
          writeStreams[prefix] = fs.createWriteStream(
            `${tilesDir}/attributes/${tileInfoPrefix}cl_${prefix}.ndjson`,
          );
          writePromises.push(
            new Promise((resolve, reject) => {
              writeStreams[prefix].on('error', err => {
                ctx.log.error(`error writing stream`, { error: err.message, stack: err.stack });
                return reject(err);
              });
              writeStreams[prefix].on('finish', () => {
                return resolve();
              });
            }),
          );
        }
        writeStreams[prefix].write(JSON.stringify(copy) + '\n', 'utf8');

        transformed++;
        if (transformed % 10000 === 0) {
          await sleep(50);
          ctx.log.info(transformed + ' records processed');
        }
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        reject(err);
      })
      .on('end', async end => {
        ctx.log.info(transformed + ' records processed');

        ctx.log.info('waiting for streams to finish writing');

        // for each stream, close it.

        ctx.log.info('writeStream keys: ', { keys: Object.keys(writeStreams) });

        Object.keys(writeStreams).forEach(key => {
          writeStreams[key].end();
        });

        await Promise.all(writePromises)
          .then(() => {
            ctx.log.info('write streams have all closed successfully');
            unwindStack(ctx.process, 'writeTileAttributes');
            resolve('end');
          })
          .catch(err => {
            reject(err);
          });
      });
  });
};

exports.extractPointsFromNdGeoJson = async function (ctx, outputPath) {
  ctx.process.push('extractPointsFromNdGeoJson');

  return new Promise((resolve, reject) => {
    ctx.log.info('extracting points from ndgeojson...');

    let transformed = 0;
    ctx.log.info('extractPointsFromNdGeoJson');
    ctx.log.info('outputPath', { outputPath });
    const points = []; // point centroid geojson features with parcel-outlet ids
    let propertyCount = 1; // will be updated below.  count of property attributes per feature.  used for deciding attribute file size and number of clusters

    fs.createReadStream(`${outputPath}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', async function (obj) {
        // create a point feature
        const pt = createPointFeature(ctx, obj);
        if (pt) {
          points.push(pt);
        }

        transformed++;
        if (transformed % 10000 === 0) {
          // may as well do this here (it will happen on feature 0 at the least)
          propertyCount = Object.keys(obj.properties).length;
          ctx.log.info(transformed + ' records processed');
          await sleep(50);
        }
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('end', end => {
        ctx.log.info(transformed + ' records processed');
        unwindStack(ctx.process, 'extractPointsFromNdGeoJson');
        return resolve([points, propertyCount]);
      });
  });
};

exports.createNdGeoJsonWithClusterId = async function (ctx, outputPath, lookup) {
  ctx.process.push('createNdGeoJsonWithClusterId');

  return new Promise((resolve, reject) => {
    ctx.log.info('createNdGeoJsonWithClusterId: creating derivative ndgeojson with clusterId...');

    let transformed = 0;

    ctx.log.info('outputPath:', { outputPath });
    const derivativePath = `${outputPath}-cluster`;

    const writer = fs.createWriteStream(`${derivativePath}.ndgeojson`);

    writer.on('finish', () => {
      ctx.log.info(`finished writing ${derivativePath}.ndgeojson`);
      unwindStack(ctx.process, 'createNdGeoJsonWithClusterId');
      return resolve(derivativePath);
    });

    fs.createReadStream(`${outputPath}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', async function (obj) {
        // add clusterId
        obj.properties = { ...obj.properties, [clusterPrefix]: lookup[obj.properties[idPrefix]] };

        writer.write(JSON.stringify(obj) + '\n', 'utf8');

        transformed++;
        if (transformed % 1000 === 0) {
          await sleep(50);
          ctx.log.info(transformed + ' records processed');
        }
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        reject(err);
      })
      .on('end', end => {
        ctx.log.info('end', { end });
        ctx.log.info(transformed + ' records processed');
        writer.end();
      });
  });
};
