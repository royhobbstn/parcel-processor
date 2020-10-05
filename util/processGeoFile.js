// @ts-check
const fs = require('fs');
const spawn = require('child_process').spawn;
const ndjson = require('ndjson');
var turf = require('@turf/turf');
const geojsonRbush = require('geojson-rbush').default;
const { StatContext } = require('./StatContext');
const {
  directories,
  tileInfoPrefix,
  idPrefix,
  clusterPrefix,
  fileFormats,
  zoomLevels,
} = require('./constants');
const { unwindStack } = require('./misc');

const HUGE_THRESHOLD = 94684125;

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
  await convertToFormat(ctx, fileFormats.NDGEOJSON, outputPath, inputPath, chosenLayerName);

  const stats = await countStats(ctx, outputPath + '.ndgeojson');

  fs.writeFileSync(outputPath + '.json', JSON.stringify(stats), 'utf8');

  unwindStack(ctx.process, 'parseFileExec');
  return;
};

exports.addUniqueIdNdjson = function (ctx, inputPath, outputPath) {
  ctx.process.push('addUniqueIdNdjson');

  const tempstore = [];

  const tree = geojsonRbush(); // to detect duplicates
  const additionalFeatures = {}; // key[id] = [array of feature properties]  : duplicate features
  let additionalCount = 0;
  let filteredOut = 0;

  return new Promise((resolve, reject) => {
    let transformed = 0;

    const writeStream = fs
      .createWriteStream(`${outputPath}.ndgeojson`)
      .on('error', err => {
        ctx.log.error('Error: ', { error: err.message, stack: err.stack });
        return reject(err);
      })
      .on('finish', () => {
        ctx.log.info(`processing complete. autoincrement id: ${idPrefix} added.`);
        ctx.log.info(`misc filtered out: ${filteredOut}`);
        ctx.log.info(`additional feature count (overlaps): ${additionalCount}`);
        unwindStack(ctx.process, 'addUniqueIdNdjson');
        return resolve(additionalFeatures);
      });

    const readStream = fs
      .createReadStream(`${inputPath}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', async function (obj) {
        readStream.pause();

        if (!obj.geometry) {
          filteredOut++;
          readStream.resume();
          return;
        }

        let flattened;
        try {
          flattened = turf.flatten(obj);
        } catch (e) {
          ctx.log.error('Error in flatten', { message: e.message, obj });
          throw e;
        }

        for (const feature of flattened.features) {
          if (!feature.geometry) {
            filteredOut++;
            continue;
          }

          // filter out impossibly tiny areas < 1m
          const featArea = turf.area(feature);
          if (featArea < 1) {
            filteredOut++;
            continue;
          }

          let featureOutline;

          // get outline from main feature
          try {
            // @ts-ignore
            featureOutline = turf.polygonToLine(feature);
            const outlineLength = turf.length(featureOutline);
            const ratio = featArea / outlineLength;
            const enhancedRatio = Math.sqrt(featArea) / outlineLength;

            const bbox = turf.bbox(feature);
            const polygon = turf.bboxPolygon(bbox);
            const polygonArea = turf.area(polygon);
            const areaRatio = featArea / polygonArea;

            // excluding oddly shaped features - usually right-of-way
            // slows down javascript processing too much
            // can think to re-include if re-written in golang
            if (outlineLength > 5 && areaRatio < 0.3 && (ratio < 10000 || enhancedRatio < 50)) {
              feature.properties.ratio = ratio;
              feature.properties.outlineLength = outlineLength;
              feature.properties.featArea = featArea;
              feature.properties.enhancedRatio = enhancedRatio;
              feature.properties.areaRatio = areaRatio;

              tempstore.push(feature);
              ctx.log.info('excluding', {
                featArea,
                outlineLength,
                ratio,
                enhancedRatio,
                polygonArea,
                areaRatio,
              });

              filteredOut++;

              continue;
            }
          } catch (e) {
            ctx.log.warn('Error in turf transformations.  Skipping', {
              message: e.message,
              feature,
            });

            filteredOut++;
            continue;
          }

          // so as to check for overlaps
          let results = {};

          // filter out huge parcels... dont expect duplicates of those
          // if there are, so be it.
          // mainly concerned about filtering out stacked apartments and condos
          if (featArea > HUGE_THRESHOLD) {
            results.features = [];
          } else {
            results = tree.search(feature);
          }

          transformed++;

          feature.properties[idPrefix] = transformed;

          let writeNewFeature = true;

          if (results.features.length) {
            for (let indexFeature of results.features) {
              let intersection;
              try {
                // @ts-ignore
                intersection = turf.intersect(indexFeature, feature);
              } catch (e) {
                ctx.log.warn('Error in turf.intersect.  This is common and ignored.');
                continue;
              }

              if (intersection) {
                const intersectionArea = turf.area(intersection);
                const ratioArea = featArea / intersectionArea;

                if (ratioArea > 0.9 && ratioArea < 1.1) {
                  additionalCount++;

                  //  get id of index feature
                  const id = indexFeature.properties[idPrefix];

                  // add current features attributes to key(id): [array of additional features]
                  if (additionalFeatures[id]) {
                    additionalFeatures[id].push(feature.properties);
                  } else {
                    additionalFeatures[id] = [feature.properties];
                  }

                  // dont write new feature to ndgeojson
                  writeNewFeature = false;
                  break;
                }
              }
            }
          }

          if (writeNewFeature) {
            const continueWriting = writeStream.write(JSON.stringify(feature) + '\n', 'utf8');
            if (!continueWriting) {
              await new Promise(resolve => writeStream.once('drain', resolve));
            }

            // still write the big features, but dont index them
            if (featArea <= HUGE_THRESHOLD) {
              tree.insert(feature);
            }
          }

          if (transformed % 1000 === 0) {
            ctx.log.info(transformed + ' records read');
          }
        }

        readStream.resume();
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('end', async () => {
        ctx.log.info(transformed + ' records read');
        ctx.log.info(`excluded ${tempstore.length} oddly shaped features`);
        // fs.writeFileSync('rati2.json', JSON.stringify(turf.featureCollection(tempstore)), 'utf8');
        writeStream.end();
      });
  });
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
      '-progress',
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

exports.tileJoinLayers = function (ctx, tilesDir) {
  ctx.process.push('tileJoinLayers');

  return new Promise((resolve, reject) => {
    const application = 'tile-join';
    const args = ['-e', tilesDir, '-pk'];
    for (let i = zoomLevels.LOW; i <= zoomLevels.HIGH; i++) {
      if (i % 2 === 0) {
        args.push(`${directories.tilesDir + ctx.directoryId}/lvl_${i}.mbtiles`);
      }
    }
    const command = `${application} ${args.join(' ')}`;
    ctx.log.info(`running: ${command}`);

    const proc = spawn(application, args);

    proc.stdout.on('data', data => {
      ctx.log.info(data.toString());
    });

    proc.stderr.on('data', data => {
      // mostly noise
      // console.log(data.toString());
    });

    proc.on('error', err => {
      ctx.log.error('Error', { err: err.message, stack: err.stack });
      reject(err);
    });

    proc.on('close', code => {
      ctx.log.info(`finished creating directory of tiles. code ${code}`);
      unwindStack(ctx.process, 'tileJoinLayers');
      resolve({ command });
    });
  });
};

exports.writeTileAttributes = function (ctx, derivativePath, tilesDir, lookup, additionalFeatures) {
  ctx.process.push('writeTileAttributes');

  return new Promise((resolve, reject) => {
    ctx.log.info('processing attributes...');

    // make attributes directory
    fs.mkdirSync(`${tilesDir}/attributes`);

    let transformed = 0;

    const writeStreams = {};
    const writePromises = [];

    const readStream = fs
      .createReadStream(`${derivativePath}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', async function (obj) {
        readStream.pause();
        const properties = obj.properties;
        properties.overlappingFeatures = additionalFeatures[properties[idPrefix]];
        const prefix = lookup[properties[idPrefix]];

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
        const continueWriting = writeStreams[prefix].write(
          JSON.stringify(properties) + '\n',
          'utf8',
        );
        if (!continueWriting) {
          await new Promise(resolve => writeStreams[prefix].once('drain', resolve));
        }

        transformed++;
        if (transformed % 10000 === 0) {
          ctx.log.info(transformed + ' records processed');
        }

        readStream.resume();
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

exports.execGolangClusters = async function (ctx, numClusters, inputFilename, outputFilename) {
  ctx.process.push('execGolangClusters');

  return new Promise((resolve, reject) => {
    const application = 'go';
    const args = [
      'run',
      './golang/cluster.go',
      numClusters,
      `${inputFilename}`,
      `${outputFilename}`,
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
      ctx.log.info(`finished running golang clusters script. code ${code}`);
      unwindStack(ctx.process, 'execGolangClusters');
      resolve({ command });
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
    const barePts = [];
    let propertyCount = 1; // will be updated below.  count of property attributes per feature.  used for deciding attribute file size and number of clusters

    const readStream = fs
      .createReadStream(`${outputPath}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', async function (obj) {
        readStream.pause();

        const pt = createPointFeature(ctx, obj);

        if (pt) {
          points.push(pt);
          barePts.push({
            id: pt.properties[idPrefix],
            lat: pt.geometry.coordinates[1],
            lng: pt.geometry.coordinates[0],
          });
        }

        transformed++;
        if (transformed % 10000 === 0) {
          // may as well do this here (it will happen on feature 0 at the least)
          propertyCount = Object.keys(obj.properties).length;
          ctx.log.info(transformed + ' records processed');
        }

        readStream.resume();
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('end', end => {
        ctx.log.info(transformed + ' records processed');
        unwindStack(ctx.process, 'extractPointsFromNdGeoJson');
        const centroidsFilename = `${directories.productTempDir + ctx.directoryId}/centroids.json`;
        fs.writeFileSync(centroidsFilename, JSON.stringify(barePts));
        return resolve([points, propertyCount, centroidsFilename]);
      });
  });
};

exports.createClusterIdHull = async function (ctx, outputPath, lookup) {
  ctx.process.push('createClusterIdHull');

  return new Promise((resolve, reject) => {
    ctx.log.info('creating clusterID Hull');

    let transformed = 0;
    const clusterGeo = {};

    fs.createReadStream(`${outputPath}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', async function (obj) {
        // add clusterId
        const cluster = lookup[obj.properties[idPrefix]];
        obj.properties = { [clusterPrefix]: cluster };

        // keep track of geojson layers in memory (hrm.. does it scale)
        if (!clusterGeo[cluster]) {
          clusterGeo[cluster] = [obj];
        } else {
          clusterGeo[cluster].push(obj);
        }

        transformed++;
        if (transformed % 5000 === 0) {
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

        const hulls = Object.keys(clusterGeo).map(key => {
          const layer = turf.convex(turf.featureCollection(clusterGeo[key]));
          layer.properties = { [clusterPrefix]: key };
          return layer;
        });
        // fs.writeFileSync('./clusterio.geojson', JSON.stringify(turf.featureCollection(hulls)));
        unwindStack(ctx.process, 'createClusterIdHull');
        resolve(turf.featureCollection(hulls));
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

async function countStats(ctx, path) {
  ctx.process.push('countStats');
  const statCounter = new StatContext(ctx, path);
  await statCounter.init();
  const stats = statCounter.exportStats();
  unwindStack(ctx.process, 'countStats');
  return stats;
}

exports.writeMbTiles = async function (ctx, filename, currentZoom) {
  // //
  ctx.process.push('writeMbTiles');

  ctx.log.info('Writing tile level ' + currentZoom);

  return new Promise((resolve, reject) => {
    const layername = 'parcelslayer';
    const application = 'tippecanoe';
    const args = [
      '-f',
      '-l',
      layername,
      '-o',
      `${directories.tilesDir + ctx.directoryId}/lvl_${currentZoom}.mbtiles`,
      `--include=${idPrefix}`,
      `--include=${clusterPrefix}`,
      '-D10',
      '-pf',
      '-pk',
      `-z${currentZoom}`,
      `-Z${currentZoom}`,
      filename,
    ];
    const command = `${application} ${args.join(' ')}`;
    ctx.log.info(`running: ${command}`);

    const proc = spawn(application, args);

    proc.stdout.on('data', data => {
      ctx.log.info(data.toString());
    });

    proc.stderr.on('data', data => {
      // mostly noise
      // console.log(data.toString());
    });

    proc.on('error', err => {
      ctx.log.error('Error', { err: err.message, stack: err.stack });
      reject(err);
    });

    proc.on('close', code => {
      ctx.log.info(`completed creating tiles. code ${code}`);
      unwindStack(ctx.process, 'writeMbTiles');
      resolve({ command, layername });
    });
  });
  //
};

// make mini ndgeojson files; 1 per each main cluster
exports.divideIntoClusters = async function (ctx, augmentedBase, miniNdgeojsonBase, lookup) {
  ctx.process.push('divideIntoClusters');

  ctx.log.info(`creating directory: ${miniNdgeojsonBase}`);
  fs.mkdirSync(miniNdgeojsonBase);

  const attributeLookupFile = {};
  const fileNames = {};
  let completedFileWrites = 0;

  await new Promise((resolve, reject) => {
    const readStream = fs
      .createReadStream(`${augmentedBase}.ndgeojson`)
      .pipe(ndjson.parse())
      .on('data', async function (obj) {
        readStream.pause();

        // save attributes to lookup file
        attributeLookupFile[obj.properties[idPrefix]] = JSON.parse(JSON.stringify(obj.properties));

        const cluster = lookup[obj.properties[idPrefix]];
        const filename = `${miniNdgeojsonBase}/file_${cluster}.ndgeojson`;

        if (!fileNames[filename]) {
          const stream = fs
            .createWriteStream(filename, { flags: 'a', emitClose: true })
            .on('error', err => {
              ctx.log.error('Error', { err: err.message, stack: err.stack });
              reject(err);
            })
            .on('close', async () => {
              completedFileWrites++;
            });
          fileNames[filename] = stream;
        }

        const continueWriting = fileNames[filename].write(JSON.stringify(obj) + '\n');
        if (!continueWriting) {
          await new Promise(resolve => fileNames[filename].once('drain', resolve));
        }
        readStream.resume();
      })
      .on('error', err => {
        ctx.log.error('Error', { err: err.message, stack: err.stack });
        return reject(err);
      })
      .on('end', async () => {
        ctx.log.info('done writing streams');
        return resolve();
      });
  });

  const streams = Object.keys(fileNames);
  streams.forEach(stream => {
    fileNames[stream].end();
  });

  await new Promise((resolve, reject) => {
    const interval = setInterval(() => {
      ctx.log.info('awaiting file writes');
      if (streams.length === completedFileWrites) {
        clearInterval(interval);
        resolve('done!!');
      }
    }, 100);
  });
  unwindStack(ctx.process, 'divideIntoClusters');
  return [streams, attributeLookupFile];
};

// write from cluster inputs ex: /files/staging/productTemp-9258e1/aggregated
// where files look like:
// '10_0.json'  '12_0.json'  '4_0.json'  '6_0.json'  '8_0.json'
// '10_1.json'  '12_1.json'  '4_1.json'  '6_1.json'  '8_1.json'

// write to aggregated files ex: /files/staging/productTemp-640843/aggregated_4.json
// `${directories.productTempDir + ctx.directoryId}/aggregated_${currentZoom}.json`
exports.aggregateAggregatedClusters = async function (ctx) {
  ctx.process.push('aggregateAggregatedClusters');

  // loop through zoom levels by 2 and aggregate all except full zoom
  // full zoom already available as ndgeojson, so no worries

  // get all files in directory
  const files = fs.readdirSync(
    `${directories.productTempDir + ctx.directoryId}/aggregated`,
    'utf8',
  );

  for (
    let currentZoom = zoomLevels.LOW;
    currentZoom <= zoomLevels.HIGH;
    currentZoom = currentZoom + 2
  ) {
    const streamOriginalFile = currentZoom === zoomLevels.HIGH;

    if (!streamOriginalFile) {
      await new Promise(async (bigResolve, bigReject) => {
        // designate writeStream for output file
        const writeStream = fs.createWriteStream(
          `${directories.productTempDir + ctx.directoryId}/aggregated_${currentZoom}.json`,
        );

        writeStream.on('error', err => {
          bigReject(err);
        });

        writeStream.on('finish', () => {
          ctx.log.info(
            `writeStream to ${
              directories.productTempDir + ctx.directoryId
            }/aggregated_${currentZoom}.json has completed writing successfully.`,
          );
          bigResolve();
        });

        // find all files in directory with pattern of `${currentZoom}_`
        const filteredFiles = files.filter(file => {
          return file.startsWith(`${currentZoom}_`);
        });

        for (const file of filteredFiles) {
          await new Promise((resolve, reject) => {
            const readStream = fs
              .createReadStream(
                `${directories.productTempDir + ctx.directoryId}/aggregated/${file}`,
              )
              .pipe(ndjson.parse())
              .on('data', async function (obj) {
                readStream.pause();
                const continueWriting = writeStream.write(JSON.stringify(obj) + '\n');
                if (!continueWriting) {
                  await new Promise(resolve => writeStream.once('drain', resolve));
                }
                readStream.resume();
              })
              .on('error', err => {
                ctx.log.error('Error', { err: err.message, stack: err.stack });
                return reject(err);
              })
              .on('end', async () => {
                return resolve();
              });
          });
        }
        writeStream.end();
      });
    }
  }

  ctx.log.info('finished aggregating aggregated clusters.');
  unwindStack(ctx.process, 'aggregateAggregatedClusters');
};
