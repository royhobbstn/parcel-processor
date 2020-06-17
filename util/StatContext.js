// @ts-check

const { idPrefix } = require('./constants');

// gather field statistics for a dataset

exports.StatContext = function () {
  this.rowCount = 0;
  this.fields = null;

  this.export = () => {
    // TODO you can do a cleanup by determining ObjectID type fields here and just
    // squashing the data down to nothing.

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
          };
        });
    }

    Object.keys(row.properties).forEach(f => {
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

      // caps uniques to 500.  Prevents things like ObjectIDs being catalogued extensively.
      if (uniques.length < 500) {
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
