var fs = require('fs');

exports.moveFile = function (oldPath, newPath) {
  return new Promise((resolve, reject) => {
    fs.rename(oldPath, newPath, err => {
      if (err) {
        if (err.code === 'EXDEV') {
          copy();
        } else {
          reject(err);
        }
      }
      resolve();
    });

    function copy() {
      var readStream = fs.createReadStream(oldPath);
      var writeStream = fs.createWriteStream(newPath);

      readStream.on('error', err => {
        reject(err);
      });
      writeStream.on('error', err => {
        reject(err);
      });
      readStream.on('close', function () {
        fs.unlink(oldPath, () => {
          resolve();
        });
      });

      readStream.pipe(writeStream);
    }
  });
};
