// @ts-check

const fs = require('fs');
const spawn = require('child_process').spawn;
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

function getOgrInfo(ctx, filePath) {
  ctx.process.push('getOgrInfo');
  let textOutput = '';

  return new Promise((resolve, reject) => {
    const application = 'ogrinfo';
    const args = ['-ro', '-al', '-so', filePath];

    const command = `${application} ${args.join(' ')}`;
    ctx.log.info(`running: ${command}`);

    const proc = spawn(application, args);

    proc.stdout.on('data', data => {
      textOutput += data.toString();
    });

    proc.stderr.on('data', data => {
      ctx.log.warn(data.toString());
    });

    proc.on('error', err => {
      ctx.log.error('Error', { err: err.message, stack: err.stack });
      return reject(err);
    });

    proc.on('close', code => {
      ctx.log.info(`completed gathering ogrinfo.`);
      unwindStack(ctx.process, 'getOgrInfo');
      ctx.log.info('command', { command });
      ctx.log.info('ogrinfo', { textOutput });
      return resolve(textOutput);
    });
  });
}

function parseOgrOutput(ctx, textOutput) {
  ctx.process.push('parseOgrOutput');

  const layers = [];
  let cursor = 0;

  const LAYER_NAME = 'Layer name: ';
  const GEOMETRY = 'Geometry: ';
  const FEATURE_COUNT = 'Feature Count: ';

  do {
    const idxLN = textOutput.indexOf(LAYER_NAME, cursor);
    if (idxLN === -1) {
      break;
    }
    const brLN = textOutput.indexOf('\n', idxLN);
    const layerName = textOutput.slice(idxLN + LAYER_NAME.length, brLN);
    const idxG = textOutput.indexOf(GEOMETRY, brLN);
    const brG = textOutput.indexOf('\n', idxG);
    const geometry = textOutput.slice(idxG + GEOMETRY.length, brG);
    const idxFC = textOutput.indexOf(FEATURE_COUNT, brG);
    const brFC = textOutput.indexOf('\n', idxFC);
    const featureCount = textOutput.slice(idxFC + FEATURE_COUNT.length, brFC);

    layers.push({ type: geometry, name: layerName, count: Number(featureCount) });
    cursor = brFC;
  } while (true);

  unwindStack(ctx.process, 'parseOgrOutput');
  return layers;
}

exports.inspectFileExec = async function (ctx, inputPath) {
  ctx.process.push('inspectFileExec');

  ctx.log.info(`opening from ${inputPath}`);

  const textOutput = await getOgrInfo(ctx, inputPath);

  const layers = parseOgrOutput(ctx, textOutput);

  const layerSelection = layers
    .filter(d => {
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

  const chosenLayerName = layerSelection[0].name;

  unwindStack(ctx.process, 'inspectFileExec');
  return chosenLayerName;
};

exports.parseFileExec = async function (ctx, outputPath, inputPath, chosenLayerName) {
  ctx.process.push('parseFileExec');

  // convert from whatever to newline delimited geojson
  await convertToFormat(
    ctx,
    fileFormats.NDGEOJSON,
    outputPath + '.temp',
    inputPath,
    chosenLayerName,
  );

  const stats = await countStats(ctx, outputPath + '.temp' + '.ndgeojson');

  fs.writeFileSync(outputPath + '.json', JSON.stringify(stats), 'utf8');

  // add idPrefix to final copy of ndgeojson
  await addUniqueIdNdjson(ctx, outputPath + '.temp' + '.ndgeojson', outputPath + '.ndgeojson');

  unwindStack(ctx.process, 'parseFileExec');
  return;
};

function addUniqueIdNdjson(ctx, inputPath, outputPath) {
  ctx.process.push('addUniqueIdNdjson');

  return new Promise((resolve, reject) => {
    let transformed = 0;

    const writeStream = fs
      .createWriteStream(outputPath)
      .on('error', err => {
        ctx.log.error('Error: ', { error: err.message, stack: err.stack });
        return reject(err);
      })
      .on('finish', () => {
        ctx.log.info(`processing complete. autoincrement id: ${idPrefix} added.`);
        unwindStack(ctx.process, 'addUniqueIdNdjson');
        return resolve();
      });

    fs.createReadStream(inputPath)
      .pipe(ndjson.parse())
      .on('data', function (obj) {
        transformed++;

        obj.properties[idPrefix] = transformed;

        writeStream.write(JSON.stringify(obj) + '\n', 'utf8');

        if (transformed % 10000 === 0) {
          ctx.log.info(transformed + ' records read');
        }
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('end', async () => {
        ctx.log.info(transformed + ' records read');
        writeStream.end();
      });
  });
}

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
    ctx.log.info('ignoring error creating point feature in:', {
      [idPrefix]: parsedFeature[idPrefix],
    });
    return null;
  }
}

exports.parseOutputPath = function (ctx, fileName, fileType) {
  const SPLITTER = fileType === 'shapefile' ? '.shp' : '.gdb';
  const splitFileName = fileName.split(SPLITTER)[0];
  const outputPath = `${directories.outputDir + ctx.directoryId}/${splitFileName}`;
  return outputPath;
};

exports.convertToFormat = convertToFormat;

function convertToFormat(ctx, format, outputPath, inputPath = '', chosenLayerName = '') {
  ctx.process.push('convertToFormat');
  ctx.process.push(format.extension);

  // convert from anything into ndgeojson, geojson, gpkg, shp, etc

  return new Promise((resolve, reject) => {
    const application = 'ogr2ogr';
    const args = [
      '-fieldTypeToString',
      'DateTime',
      '-f',
      format.driver,
      `${outputPath}.${format.extension}`,
      inputPath || `${outputPath}.ndgeojson`,
    ];

    if (chosenLayerName) {
      args.push(chosenLayerName);
    }

    // geojson needs RFC option specified
    // ndgeojson doesnt accept it.
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
}

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
      '-pS',
      '-D10',
      '-M',
      '512000',
      '--coalesce-densest-as-needed',
      '--maximum-zoom=12',
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

exports.readTippecanoeMetadata = async function (ctx, metadataFile) {
  ctx.process.push('readTippecanoeMetadata');
  let metadataContents;

  try {
    metadataContents = JSON.parse(fs.readFileSync(metadataFile, 'utf8'));
  } catch (err) {
    ctx.log.error(`Error reading tippecanoe metadata file from disk.  Path: ${metadataFile}`);
    throw err;
  }

  try {
    // delete file so it doesnt get synced to S3
    fs.unlinkSync(metadataFile);
  } catch (err) {
    ctx.log.warn(
      `Proble deleting tippecanoe metadata file from disk.  Path ${metadataFile}. Not critical.  Will continue. `,
    );
  }

  unwindStack(ctx.process, 'readTippecanoeMetadata');
  return metadataContents;
};

exports.countStats = countStats;

function countStats(ctx, path) {
  ctx.process.push('countStats');

  return new Promise((resolve, reject) => {
    // read each file as ndjson and create stat object
    let transformed = 0;
    const statCounter = new StatContext(ctx);

    fs.createReadStream(path)
      .pipe(ndjson.parse())
      .on('data', function (obj) {
        statCounter.countStats(obj);

        transformed++;
        if (transformed % 10000 === 0) {
          ctx.log.info(transformed + ' records processed');
        }
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('end', async () => {
        ctx.log.info(transformed + ' records processed');
        unwindStack(ctx.process, 'countStats');
        return resolve(statCounter.export());
      });
  });
}
