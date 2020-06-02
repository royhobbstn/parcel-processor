const crypto = require('crypto');
const fs = require('fs');

exports.computeHash = function (filePath) {
  return new Promise((resolve, reject) => {
    console.log('processing file...');

    const hash = crypto.createHash('md5');
    const stream = fs.createReadStream(filePath);

    stream.on('data', data => {
      hash.update(data, 'utf8');
    });

    stream.on('error', err => {
      console.error(err);
      reject(err);
    });

    stream.on('end', () => {
      computedHash = hash.digest('hex');
      console.log(`Computed Hash: ${computedHash}`);
      resolve(computedHash);
    });
  });
};
