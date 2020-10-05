// @ts-check
const fs = require('fs');
const ndjson = require('ndjson');
const { unwindStack } = require('./misc');

// gather field statistics for a dataset

// please keep uniquesMax at 500 or higher, else sort could be affected
exports.StatContextAttribute = function (ctx, filePath, uniquesMax = 500) {
  this.rowCount = 0;
  this.fields = {};
  this.filePath = filePath;
  this.attributes = [];

  this.init = async () => {
    // get attribute list & count total records
    ctx.process.push('StatContext - init');
    ctx.log.info('starting stat context init');

    let transformed = 0;
    const attributes = {};

    await new Promise((resolve, reject) => {
      fs.createReadStream(filePath)
        .pipe(ndjson.parse())
        .on('data', function (obj) {
          Object.keys(obj.properties).forEach(f => {
            if (!attributes[f]) {
              attributes[f] = true;
            }
          });

          transformed++;
          if (transformed % 50000 === 0) {
            ctx.log.info(transformed + ' init records processed');
          }
        })
        .on('error', err => {
          ctx.log.error(err);
          reject(err);
        })
        .on('end', end => {
          ctx.log.info(transformed + ' init records processed');
          this.rowCount = transformed;
          this.attributes = Object.keys(attributes);
          resolve();
        });
    });

    // loop through stats and enumerate / crunch each one, saving back onto the statContext
    for (let attribute of this.attributes) {
      ctx.log.info(`processing ${attribute}`);

      let transformed = 0;

      // initialize attribute
      this.fields[attribute] = {
        types: [],
        uniques: new Map(),
        IDField: false,
        IDSample: [],
      };

      await new Promise((resolve, reject) => {
        const readStream = fs
          .createReadStream(filePath)
          .pipe(ndjson.parse())
          .on('data', obj => {
            this.countStat(obj, attribute);

            transformed++;
            if (transformed % 100000 === 0) {
              ctx.log.info(`${transformed} ${attribute} records processed`);
            }
          })
          .on('error', err => {
            ctx.log.error(err);
            reject(err);
          })
          .on('end', () => {
            ctx.log.info(`${transformed} ${attribute} records processed`);
            this.crunchAttribute(attribute);
            resolve();
          });
      });
    }

    ctx.log.info('Done processing attributes, ready for export.');
    unwindStack(ctx.process, 'StatContext - init');
  };

  this.countStat = (row, attribute) => {
    const value = row.properties[attribute];
    const type = typeof value;
    const strValue = String(value);

    const types = this.fields[attribute].types;

    if (type === 'string' || type === 'number') {
      if (!types.includes(type)) {
        this.fields[attribute].types.push(type);
      }
    }

    if (!this.fields[attribute].uniques.has(strValue)) {
      this.fields[attribute].uniques.set(strValue, 1);
    } else {
      this.fields[attribute].uniques.set(
        strValue,
        this.fields[attribute].uniques.get(strValue) + 1,
      );
    }

    if (type === 'number') {
      if (value > this.fields[attribute].max) {
        this.fields[attribute].max = value;
      }
      if (value < this.fields[attribute].min) {
        this.fields[attribute].min = value;
      }
    }
  };

  this.crunchAttribute = attribute => {
    // cleanup by determining ObjectID type fields here and just
    // squashing the data down to nothing.

    ctx.log.info('Squashing unique fields');

    const a = new Map();
    a.keys();
    let isUnique = true;
    for (let unique of this.fields[attribute].uniques.keys()) {
      if (this.fields[attribute].uniques.get(unique) > 1) {
        isUnique = false;
        break;
      }
    }

    if (isUnique) {
      this.fields[attribute].IDSample = Array.from(this.fields[attribute].uniques.keys()).slice(
        0,
        10,
      );
      this.fields[attribute].uniques = {};
      this.fields[attribute].IDField = true;
    }

    ctx.log.info('Processing export fields');

    ctx.log.info(`processing field: ${attribute}`);
    const currentField = this.fields[attribute];
    if (!currentField.IDField) {
      // turn it into array
      ctx.log.info(' - creating array');
      const arr = Array.from(currentField.uniques.keys()).map(key => {
        return { key, value: currentField.uniques.get(key) };
      });
      ctx.log.info(' - sorting array');
      arr.sort((a, b) => {
        return b.value > a.value ? 1 : -1;
      });
      ctx.log.info(` - keeping up to ${uniquesMax} unique values`);
      // keep up to uniquesMax Limit
      const mostFrequent = arr.slice(0, uniquesMax);
      // back to keyed object
      const obj = {};
      mostFrequent.forEach(row => {
        obj[row.key] = row.value;
      });
      // mutate original, keeping only uniquesMax values
      currentField.uniques = obj;
      ctx.log.info(`done crunching ${attribute}`);
    }
  };

  this.exportStats = () => {
    ctx.log.info('Exporting stats data.');
    return {
      rowCount: this.rowCount,
      fields: this.fields,
    };
  };
};
