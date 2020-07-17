// @ts-check

const { idPrefix } = require('./constants');
const { unwindStack } = require('./misc');

// gather field statistics for a dataset

exports.StatContext = function (ctx, uniquesMax = 500) {
  ctx.process.push('StatContext');

  this.rowCount = 0;
  this.fields = null;

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

    unwindStack(ctx.process, 'StatContext');
    return {
      rowCount: this.rowCount,
      fields: this.fields,
    };
  };

  this.countStats = row => {
    this.rowCount++;

    // add fields names one time
    if (!this.fields) {
      this.fields = {};
      // iterate through columns in each row, add as field name to summary object
      Object.keys(row.properties)
        .filter(d => d !== idPrefix)
        .forEach(f => {
          this.fields[f] = {
            types: [],
            uniques: {},
            mean: 0,
            max: -Infinity,
            min: Infinity,
            numCount: 0,
            strCount: 0,
            IDField: false,
            IDSample: [],
          };
        });
    }

    Object.keys(row.properties)
      .filter(d => d !== idPrefix)
      .forEach(f => {
        const value = row.properties[f];
        const type = typeof value;
        const types = this.fields[f].types;

        if (type === 'string' || type === 'number') {
          if (!types.includes(type)) {
            this.fields[f].types.push(type);
          }
        }

        // both strings and numbers watch uniques
        const uniques = Object.keys(this.fields[f].uniques);

        // caps uniques to uniquesMax.  Prevents things like ObjectIDs being catalogued extensively.
        if (uniques.length < uniquesMax) {
          if (!uniques.includes(String(value))) {
            this.fields[f].uniques[String(value)] = 1;
          } else {
            this.fields[f].uniques[String(value)]++;
          }
        }

        if (type === 'string') {
          this.fields[f].strCount++;
        } else if (type === 'number') {
          this.fields[f].numCount++;

          if (value > this.fields[f].max) {
            this.fields[f].max = value;
          }
          if (value < this.fields[f].min) {
            this.fields[f].min = value;
          }

          // might cause overflow here
          this.fields[f].mean =
            (this.fields[f].mean * (this.fields[f].numCount - 1) + value) / this.fields[f].numCount;
        } else {
          // probably null.  skipping;
        }
      });
  };
};
