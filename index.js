const io = require('socket.io')();
const chokidar = require('chokidar');
const crypto = require('crypto');
const fs = require('fs');
const unzipper = require('unzipper');
const shapefile = require("shapefile");
const gdal = require("gdal-next");

const rawDir = './raw';
const unzippedDir = './unzipped';
const outputDir = './output';

// watch filesystem and compute hash of incoming file.
chokidar.watch(rawDir, {
  ignored: /(^|[\/\\])\../, // ignore dotfiles
  awaitWriteFinish: true
}).on('add', (path) => {
  // todo this is a short circuit - remove
  return checkForFileType();

  console.log(`\nFile found: ${path}`);

  // todo send message to client that file is being processed

  const hash = crypto.createHash('md5');
  const stream = fs.createReadStream(path);

  stream.on('data', (data) => {
    hash.update(data, 'utf8');
  });

  stream.on('end', () => {

    computedHash = hash.digest('hex');
    console.log(`Computed Hash: ${computedHash}`);

    // todo send message to client that hash has been computed

    checkDBforHash(path, computedHash);
  });

});

function checkDBforHash(path, computedHash) {

  // todo write page_check record to serverless aurora

  // todo call serverless aurora and find out if hash is already in DB

  // if is in database, END

  // if not in database write a record in the download table

  // then extract
  extractZip(path, computedHash);
}

function extractZip(path, computedHash) {
  fs.createReadStream(path)
  .pipe(unzipper.Extract({ path: unzippedDir }))
  .on('error', (err) => {
    console.error(`Error unzipping file: ${path}`)
    console.error(err);
    process.exit();
  })
  .on('close', () => {
    console.log(`Finished unzipping file: ${path}`);
    checkForFileType();
  });
}

// determine if the unzipped folder contains a shapefile or FGDB
function checkForFileType() {

  try {
    const arrayOfFiles = fs.readdirSync(unzippedDir)
    console.log(arrayOfFiles)

    // determine if it's a shapefile by examining files in directory and looking for .shp
    // noting that there could possibly be multiple shapefiles in a zip archive
    const shpFilenames = new Set();
    const gdbFilenames = new Set();

    arrayOfFiles.forEach(file => {
      if(file.includes(".shp")) {
        const filename = file.split(".shp")[0];
        shpFilenames.add(filename);
      }
      if(file.includes(".gdb")) {
        gdbFilenames.add(file);
      }
    });

    if(shpFilenames.size === 0) {
      // TODO no shapefiles found

      if(gdbFilenames.size === 1) {
        const gdb = Array.from(gdbFilenames)[0];
        console.log(`Found geodatabase: ${gdb}`);
        const dataset = gdal.open(`${unzippedDir}/${gdb}`);
        let i=0;

const driver = dataset.driver
const driver_metadata = driver.getMetadata()
if (driver_metadata.DCAP_VECTOR !== 'YES') {
  console.error('Source file is not a vector')
  process.exit(1)
}

console.log(`Driver = ${driver.description}`)
console.log('')
        dataset.layers.forEach(layer => {

          console.log(`${i++}: ${layer.name}`)

          console.log(`  Geometry Type = ${gdal.Geometry.getName(layer.geomType)}`)
          console.log(
            `  Spatial Reference = ${layer.srs ? layer.srs.toWKT() : 'null'}`
          )
        
          if(layer.srs) {
            const extent = layer.getExtent()
            console.log('  Extent: ')
            console.log(`    minX = ${extent.minX}`)
            console.log(`    minY = ${extent.minY}`)
            console.log(`    maxX = ${extent.maxX}`)
            console.log(`    maxY = ${extent.maxY}`)
          }

        
          console.log('  Fields: ')
          layer.fields.forEach((field) => {
            console.log(`    -${field.name} (${field.type})`)
          })
        
          console.log(`  Feature Count = ${layer.features.count()}`)
        });

      } else {
        console.error('must be something else.  TODO.');
        process.exit();
      }

    }

    if(shpFilenames.size > 1) {
      // TODO multiple shapefiles
      console.error('multiple shapefiles.  TODO.');
      process.exit();
    }

    if(shpFilenames.size === 1) {
      processShp(Array.from(filenames)[0]);
    }


  } catch(err) {
    console.error(`Error reading directory: ${unzippedDir}`);
    console.error(err);
    process.exit();
  }

}



function processShp(filename_root) {

  const statCounter = new StatContext();

  console.log(`processing shapefile: ${filename_root}.shp`);

  let writeStream = fs.createWriteStream(
    `${outputDir}/${filename_root}.ndgeojson`
  );

  shapefile
    .open(`${unzippedDir}/${filename_root}.shp`)
    .then((source) =>
      source.read().then(function log(result) {
        if (result.done) {
          writeStream.end();
          return;
        }
        statCounter.countStats(result.value);
        writeStream.write(JSON.stringify(result.value) + "\n", "utf8");
        return source.read().then(log);
      })
    )
    .catch((error) => console.error(error.stack));

  // the finish event is emitted when all data has been flushed from the stream
  writeStream.on("finish", () => {
    fs.writeFileSync(
      `${outputDir}/${filename_root}.json`,
      JSON.stringify(statCounter.export()),
      "utf8"
    );
    console.log(
      `wrote all ${statCounter.rowCount} rows to file: ${outputDir}/${filename_root}.ndgeojson`
    );
  });
}


function StatContext() {
  this.rowCount = 0;
  this.fields = null;

  this.export = () => {
    return {
      rowCount: this.rowCount,
      fields: this.fields,
    };
  };

  this.countStats = (row) => {
    this.rowCount++;

    // add fields names one time
    if (!this.fields) {
      this.fields = {};
      // iterate through columns in each row, add as field name to summary object
      Object.keys(row.properties).forEach((f) => {
        this.fields[f] = {
          types: [],
          uniques: {},
          mean: 0,
          max: 0,
          min: 0,
          numCount: 0,
          strCount: 0,
        };
      });
    }

    Object.keys(row.properties).forEach((f) => {
      const value = row.properties[f];
      const type = typeof value;
      const types = this.fields[f].types;

      if (type === "string" || type === "number") {
        if (!types.includes(type)) {
          this.fields[f].types.push(type);
        }
      }

      if (type === "string") {
        this.fields[f].strCount++;

        const uniques = Object.keys(this.fields[f].uniques);

        // caps uniques to 1000.  Prevents things like ObjectIDs being catalogued extensively.
        if (uniques.length < 1000) {
          if (!uniques.includes(value)) {
            this.fields[f].uniques[value] = 1;
          } else {
            this.fields[f].uniques[value]++;
          }
        }
      } else if (type === "number") {
        this.fields[f].numCount++;

        if (value > this.fields[f].max) {
          this.fields[f].max = value;
        }
        if (value < this.fields[f].min) {
          this.fields[f].min = value;
        }

        // might cause overflow here
        this.fields[f].mean =
          (this.fields[f].mean * (this.fields[f].numCount - 1) + value) /
          this.fields[f].numCount;
      } else {
        // probably null.  skipping;
      }
    });
  };
}


io.on('connection', (client) => {
  client.on('subscribeToTimer', (interval) => {
    console.log('client is subscribing to timer with interval ', interval);
    setInterval(() => {
      client.emit('timer', new Date());
    }, interval);
  });
});

const port = 8000;
io.listen(port);
console.log('listening on port ', port);