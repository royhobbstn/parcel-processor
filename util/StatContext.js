// @ts-check
const fs = require('fs');
const ndjson = require('ndjson');
const { unwindStack, getTimestamp } = require('./misc');

// gather field statistics for a dataset

exports.StatContext = function (ctx, filePath, uniquesMax = 500) {
  this.rowCount = 0;
  this.fields = {};
  this.filePath = filePath;

  this.init = async () => {
    // get attribute list & count total records
    ctx.process.push({ name: 'StatContext - init', timestamp: getTimestamp() });
    ctx.log.info('starting stat context init');

    await new Promise((resolve, reject) => {
      const readStream = fs
        .createReadStream(filePath)
        .pipe(ndjson.parse({ strict: false }))
        .on('data', obj => {
          this.countStats(obj);

          if (this.rowCount % 100000 === 0) {
            ctx.log.info(`${this.rowCount} ndgeojson records processed`);
          }
        })
        .on('error', err => {
          ctx.log.warn('Error', { err: err.message, stack: err.stack });
          return reject(err);
        })
        .on('end', () => {
          ctx.log.info(`reading complete. beginning to process...`);
          this.crunchAttributes();
          resolve();
        });
    });

    ctx.log.info('Done processing attributes, ready for export.');
    unwindStack(ctx, 'StatContext - init');
  };

  this.crunchAttributes = () => {
    // cleanup by determining ObjectID type fields here and just
    // squashing the data down to nothing.

    Object.keys(this.fields).forEach(f => {
      ctx.log.info('Squashing unique field ' + f);

      let isUnique = true;
      for (let unique of this.fields[f].uniques.keys()) {
        if (this.fields[f].uniques.get(unique) > 1) {
          isUnique = false;
          break;
        }
      }

      if (isUnique) {
        this.fields[f].IDSample = Array.from(this.fields[f].uniques.keys()).slice(0, 10);
        this.fields[f].uniques = {};
        this.fields[f].IDField = true;
      }

      ctx.log.info(`processing field: ${f}`);
      const currentField = this.fields[f];
      if (!currentField.IDField) {
        // turn it into array
        const arr = Array.from(currentField.uniques.keys()).map(key => {
          return { key, value: currentField.uniques.get(key) };
        });
        arr.sort((a, b) => {
          return b.value > a.value ? 1 : -1;
        });
        // keep up to uniquesMax Limit
        const mostFrequent = arr.slice(0, uniquesMax);
        // back to keyed object
        const obj = {};
        mostFrequent.forEach(row => {
          obj[row.key] = row.value;
        });
        // mutate original, keeping only uniquesMax values
        currentField.uniques = obj;
      }
    });
  };

  this.exportStats = () => {
    ctx.log.info('Exporting stats data.');
    return {
      rowCount: this.rowCount,
      fields: this.fields,
    };
  };

  this.countStats = row => {
    this.rowCount++;

    Object.keys(row.properties).forEach(f => {
      const value = row.properties[f];
      const type = typeof value;
      const strValue = String(value);

      if (!this.fields[f]) {
        this.fields[f] = {
          max: undefined,
          min: undefined,
          types: [],
          uniques: new Map(),
          IDField: false,
          IDSample: [],
        };
      }

      const types = this.fields[f].types;

      if (type === 'string' || type === 'number') {
        if (!types.includes(type)) {
          this.fields[f].types.push(type);
        }
      }

      // increment each unique
      if (!this.fields[f].uniques.has(strValue)) {
        this.fields[f].uniques.set(strValue, 1);
      } else {
        this.fields[f].uniques.set(strValue, this.fields[f].uniques.get(strValue) + 1);
      }

      if (type === 'number') {
        if (this.fields[f].max === undefined) {
          // initial
          this.fields[f].max = value;
          this.fields[f].min = value;
        } else {
          if (value > this.fields[f].max) {
            this.fields[f].max = value;
          }
          if (value < this.fields[f].min) {
            this.fields[f].min = value;
          }
        }
      }
    });
  };
};
