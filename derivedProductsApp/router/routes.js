exports.appRouter = app => {
  //
  let counter = 0;
  app.get('/queryStatFiles', function (req, res) {
    console.log('fips: ', req.query.fips);
    counter++;
    iterator = counter % 5;

    // query for ndgeojson files (as proxy for -stat files)

    if (iterator === 0) {
      res.send([]);
    }

    return res.send(
      [
        {
          fips: '08031',
          geoname: 'Colorado',
          downloadRef: 'JK78KJHI',
          downloadId: 123,
          productRef: 'JKSDIN78',
          productId: 234,
          productKey: '/sfsdfsdf/asdfsdf/file.ndgeojson',
          downloadDate: 'Timestamp1',
          sourceName: 'http://www.denver.com',
          sourceType: 'webpage',
          rawKey: '/sdfas/sdfsd/ksdhfkj',
          originalFilename: 'blahz.zip',
        },
        {
          fips: '08031',
          geoname: 'Colorado',
          downloadRef: 'JK78KJHI',
          downloadId: 124,
          productRef: 'JKSDIN78',
          productId: 234,
          productKey: '/sfsdfsdf/asdfsdf/file.ndgeojson',
          downloadDate: 'Timestamp1',
          sourceName: 'http://www.denver.com',
          sourceType: 'webpage',
          rawKey: '/sdfas/sdfsd/ksdhfkj',
          originalFilename: 'blahz.zip',
        },
        {
          fips: '08031',
          geoname: 'Colorado',
          downloadRef: 'JK78KJHI',
          downloadId: 125,
          productRef: 'JKSDIN78',
          productId: 234,
          productKey: '/sfsdfsdf/asdfsdf/file.ndgeojson',
          downloadDate: 'Timestamp1',
          sourceName: 'http://www.denver.com',
          sourceType: 'webpage',
          rawKey: '/sdfas/sdfsd/ksdhfkj',
          originalFilename: 'blahz.zip',
        },
        {
          fips: '08031',
          geoname: 'Colorado',
          downloadRef: 'JK78KJHI',
          downloadId: 126,
          productRef: 'JKSDIN78',
          productId: 234,
          productKey: '/sfsdfsdf/asdfsdf/file.ndgeojson',
          downloadDate: 'Timestamp1',
          sourceName: 'http://www.denver.com',
          sourceType: 'webpage',
          rawKey: '/sdfas/sdfsd/ksdhfkj',
          originalFilename: 'blahz.zip',
        },
        {
          fips: '08031',
          geoname: 'Colorado',
          downloadRef: 'JK78KJHI',
          downloadId: 127,
          productRef: 'JKSDIN78',
          productId: 234,
          productKey: '/sfsdfsdf/asdfsdf/file.ndgeojson',
          downloadDate: 'Timestamp1',
          sourceName: 'http://www.denver.com',
          sourceType: 'webpage',
          rawKey: '/sdfas/sdfsd/ksdhfkj',
          originalFilename: 'blahz.zip',
        },
      ].slice(iterator),
    );
  });
  //
};
