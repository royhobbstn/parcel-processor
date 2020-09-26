// @ts-check
const fs = require('fs');
const config = require('config');
const { acquireConnection } = require('./wrapQuery');
const {
  getDownloadsData,
  getSourceData,
  getProductsData,
  getGeoIdentifiersData,
} = require('./queries');
const { putFileToS3 } = require('./s3Operations');
const { invalidateFiles } = require('./cloudfrontOps');

const { countyNameLookup, stateNameLookup } = require('../lookup/nameLookup');
const { countyPopulation, totalPopulation, totalCountyCount } = require('../lookup/popLookup');

exports.siteData = siteData;

async function siteData(ctx) {
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
        population: countyPopulation[geoid],
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
      geoid,
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

  let populationAccountedFor = 0;
  let countiesAccountedFor = 0;

  // add last_download to each source.
  Object.keys(data).forEach(geoidKey => {
    populationAccountedFor += countyPopulation[geoidKey];
    countiesAccountedFor += 1;
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

  const percentOfTotalPopulation = populationAccountedFor / totalPopulation;
  const percentOfTotalCounties = countiesAccountedFor / totalCountyCount;

  data.popStats = {
    totalPopulation,
    totalCountyCount,
    populationAccountedFor,
    countiesAccountedFor,
    percentOfTotalPopulation,
    percentOfTotalCounties,
  };

  // top 50 highest value additions
  const highestValueCounties = Object.keys(countyPopulation)
    .sort((a, b) => countyPopulation[b] - countyPopulation[a])
    .filter(d => !data[d])
    .map((d, i) => {
      return {
        geoid: d,
        countyName: countyNameLookup(d),
        stateName: stateNameLookup(d.slice(0, 2)),
        rank: i + 1,
      };
    });

  fs.writeFileSync(config.get('DataExport.localPath'), JSON.stringify(data), 'utf8');
  fs.writeFileSync(
    config.get('DataExport.valuePath'),
    JSON.stringify(highestValueCounties.slice(0, 50), null, '  '),
    'utf8',
  );

  // production - put data on server
  if (config.get('env') === 'production') {
    await putFileToS3(
      ctx,
      config.get('Buckets.websiteBucket'),
      config.get('DataExport.remotePath'),
      config.get('DataExport.localPath'),
      'application/json',
      true,
      false,
    );
  }

  // dev and prod, copy to repo folder
  fs.copyFileSync(
    config.get('DataExport.localPath'),
    `../parcel-outlet/public/${config.get('DataExport.remotePath')}`,
  );

  if (config.get('env') === 'production') {
    await invalidateFiles(ctx, [`/${config.get('DataExport.remotePath')}`]);
  }
}
