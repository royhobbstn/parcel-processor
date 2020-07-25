// @ts-check
const fs = require('fs');
const { acquireConnection } = require('./util/wrapQuery');
const {
  getDownloadsData,
  getSourceData,
  getProductsData,
  getGeoIdentifiersData,
} = require('./util/queries');

const ctx = { log: console, process: [] };

async function main() {
  await acquireConnection(ctx);

  const downloads_data = await getDownloadsData(ctx);
  const source_data = await getSourceData(ctx);
  const products_data = await getProductsData(ctx);
  const geo_data = await getGeoIdentifiersData(ctx);

  const downloads = {};
  downloads_data.forEach(row => {
    downloads[row.download_id] = row;
  });

  const sources = {};
  source_data.forEach(row => {
    sources[row.source_id] = row;
  });

  const geo = {};
  geo_data.forEach(row => {
    geo[row.geoid] = row;
  });

  //

  const data = {};

  //   for each product, iterate
  products_data.forEach(row => {
    const geoid = row.geoid;

    if (!data[geoid]) {
      data[geoid] = {
        geoid,
        geoname: geo[geoid].geoname,
        sumlev: geo[geoid].sumlev,
        sources: {},
      };
    }

    const download_id = row.download_id;
    const source_id = downloads[download_id].source_id;

    if (!data[geoid].sources[source_id]) {
      data[geoid].sources[source_id] = {
        source_id,
        source_name: sources[source_id].source_name,
        last_checked: sources[source_id].last_checked,
        downloads: {},
      };
    }

    if (!data[geoid].sources[source_id].downloads[download_id]) {
      data[geoid].sources[source_id].downloads[download_id] = {
        download_id,
        created: downloads[download_id].created,
        download_ref: downloads[download_id].download_ref,
        raw_key: downloads[download_id].raw_key,
        products: [],
      };
    }

    // push product into proper place
    data[geoid].sources[source_id].downloads[download_id].products.push({
      product_ref: row.product_ref,
      product_origin: row.product_origin,
      product_id: row.product_id,
      product_individual_ref: row.individual_ref,
      product_type: row.product_type,
      product_key: row.product_key,
    });

    //
  });

  // Post Processing

  // no state-level datasets allowed (for now)
  Object.keys(data).forEach(key => {
    if (data[key].sumlev === '040') {
      delete data[key];
    }
  });

  // add last_download to each source.
  Object.keys(data).forEach(geoidKey => {
    Object.keys(data[geoidKey].sources).forEach(sourceIdKey => {
      const source = data[geoidKey].sources[sourceIdKey];
      const downloads = data[geoidKey].sources[sourceIdKey].downloads;
      let newestDate = '1970-01-01 12:00:00';
      Object.keys(downloads).forEach(downloadKey => {
        const download_creation = downloads[downloadKey].created;
        if (new Date(download_creation) > new Date(newestDate)) {
          newestDate = download_creation;
        }
      });
      source.last_download = newestDate;
    });
  });

  console.log(JSON.stringify(data, null, ' '));

  fs.writeFileSync('database_data.json', JSON.stringify(data, null, ' '), 'utf8');

  //
}

main();
