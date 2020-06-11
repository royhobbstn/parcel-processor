// @ts-check

const crypto = require('crypto');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const zlib = require('zlib');
const { tileInfoPrefix } = require('./constants');

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

exports.gzipTileAttributes = async function (directory) {
  const arrayOfFiles = fs.readdirSync(directory);

  const copiedFiles = arrayOfFiles
    .filter(file => {
      return file.slice(0, tileInfoPrefix.length) === tileInfoPrefix;
    })
    .map(file => {
      return convertToGzip(
        `${directory}/${file}`,
        `${directory}/${file.slice(tileInfoPrefix.length)}`,
      );
    });

  await Promise.all(copiedFiles);

  console.log('All tile information files have been gzipped');
};

function convertToGzip(oldPath, newPath) {
  return new Promise((resolve, reject) => {
    var readStream = fs.createReadStream(oldPath);
    var writeStream = fs.createWriteStream(newPath);
    const z = zlib.createGzip();

    readStream.on('error', err => {
      return reject(err);
    });
    writeStream.on('error', err => {
      return reject(err);
    });
    readStream.on('close', function () {
      fs.unlink(oldPath, () => {
        return resolve();
      });
    });

    readStream.pipe(z).pipe(writeStream);
  });
}
