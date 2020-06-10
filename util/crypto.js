// @ts-check

const crypto = require('crypto');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

exports.generateRef = function (digits) {
  const uuid = uuidv4();
  // @ts-ignore
  const plainString = uuid.replace(/-,/g);
  return plainString.slice(0, digits);
};

exports.computeHash = function (filePath) {
  return new Promise((resolve, reject) => {
    console.log('processing file...');

    const hash = crypto.createHash('md5');
    const stream = fs.createReadStream(filePath);

    stream.on('data', data => {
      // @ts-ignore
      hash.update(data, 'utf8');
    });

    stream.on('error', err => {
      console.error(err);
      return reject(err);
    });

    stream.on('end', () => {
      const computedHash = hash.digest('hex');
      console.log(`Computed Hash: ${computedHash}`);
      return resolve(computedHash);
    });
  });
};
