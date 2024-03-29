const FeatureService = require('featureservice');
const axios = require('axios').default;
const fs = require('fs');
const { promisify } = require('util');
const writeFileAsync = promisify(fs.writeFile); // (A)

// a url to a feature service
// const url = 'https://services.arcgis.com/yygmGNIVQrHqSELP/arcgis/rest/services/SLOparcles/FeatureServer/0/query?outFields=*&where=

// const url =
//   "https://gis.crcog.org/arcgis/rest/services/RegionalParcelViewer/Parcel_poly_Cached/MapServer/";

// const url =
//   'https://services3.arcgis.com/e3KSI9Py4B7m3xu6/arcgis/rest/services/02_arcgis_online_Address_and_Tax_Lots_1/FeatureServer/1/query?outFields=*&where=';

const url = 'http://lcmaps.lanecounty.org/arcgis/rest/services/PATS/LakePATSReference/MapServer/7';

const service = new FeatureService(url, {});

service.pages(function (err, pages) {
  console.log({ err });
  console.log({ pages });
  /* will give you links to all pages of data in the service*/

  const queue = [];

  pages.forEach((page, index) => {
    const geojsonUrl = page.req.replace('f=json', 'f=geoJSON');

    queue.push(
      axios
        .get(geojsonUrl)
        .then(function (response) {
          return writeFileAsync(`./${index}.geojson`, JSON.stringify(response.data));
        })
        .then(res => {
          console.log(geojsonUrl);
          console.log('write');
        })
        .catch(function (error) {
          console.log(error);
        }),
    );
  });

  Promise.all(queue)
    .then(() => {
      console.log('done');
    })
    .catch(err => {
      console.log(err);
    });
});
