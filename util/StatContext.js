// @ts-check

const { unwindStack } = require('./misc');
const { idPrefix } = require('./constants');

// gather field statistics for a dataset

exports.StatContext = function (ctx, uniquesMax = 500) {
  ctx.process.push('StatContext');

  this.rowCount = 0;
  this.fields = {};

  this.export = () => {
    // cleanup by determining ObjectID type fields here and just
    // squashing the data down to nothing.

    Object.keys(this.fields).forEach(field => {
      let isUnique = true;
      for (let unique of Object.keys(this.fields[field].uniques)) {
        if (this.fields[field].uniques[unique] > 1) {
          isUnique = false;
          break;
        }
      }

      if (isUnique) {
        this.fields[field].IDSample = Object.keys(this.fields[field].uniques).slice(0, 10);
        this.fields[field].uniques = {};
        this.fields[field].IDField = true;
      }
    });

    Object.keys(this.fields).forEach(field => {
      const currentField = this.fields[field];
      if (!currentField.IDField) {
        // turn it into array
        const arr = Object.keys(currentField.uniques).map(key => {
          return { key, value: currentField.uniques[key] };
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

    unwindStack(ctx.process, 'StatContext');
    return {
      rowCount: this.rowCount,
      fields: this.fields,
    };
  };

  this.countStats = row => {
    this.rowCount++;

    Object.keys(row.properties).forEach(f => {
      if (f === idPrefix) {
        return;
      }
      const value = row.properties[f];
      const type = typeof value;
      const strValue = String(value);

      if (!this.fields[f]) {
        this.fields[f] = {
          types: [],
          uniques: {},
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
      if (!this.fields[f].uniques[strValue]) {
        this.fields[f].uniques[strValue] = 1;
      } else {
        this.fields[f].uniques[strValue]++;
      }

      if (type === 'number') {
        if (value > this.fields[f].max) {
          this.fields[f].max = value;
        }
        if (value < this.fields[f].min) {
          this.fields[f].min = value;
        }

        // might cause overflow here
        this.fields[f].mean =
          (this.fields[f].mean * (this.fields[f].numCount - 1) + value) / this.fields[f].numCount;
      }
    });
  };
};
