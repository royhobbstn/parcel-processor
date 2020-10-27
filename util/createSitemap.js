const fs = require('fs');
const { countyNameLookup, stateNameLookup, countySubNameLookup } = require('../lookup/nameLookup');

exports.createSitemap = createSitemap;

function createSitemap(ctx, data) {
  const dateRaw = new Date();
  const currentDate = dateRaw.toISOString().slice(0, 10);

  const sitesData = data.map(d => {
    return `<url>
  <loc>https://www.parcel-outlet.com/parcel-map/${lookupName(d)}/${d}</loc>
  <lastmod>${currentDate}</lastmod>
  <changefreq>monthly</changefreq>
  <priority>0.1</priority>
</url>
`;
  });

  //

  const siteMap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url>
  <loc>https://www.parcel-outlet.com/</loc>
  <lastmod>${currentDate}</lastmod>
  <changefreq>weekly</changefreq>
  <priority>1</priority>
</url>
<url>
  <loc>https://www.parcel-outlet.com/coverage-map</loc>
  <lastmod>${currentDate}</lastmod>
  <changefreq>weekly</changefreq>
  <priority>1</priority>
</url>
${sitesData}
</urlset>`;

  return siteMap;
}

function lookupName(geoid) {
  const state = geoid.slice(0, 2);
  let geoname;
  if (geoid.length === 10) {
    geoname = `${countySubNameLookup(geoid)} ${stateNameLookup(state)}`;
  } else {
    geoname = `${countyNameLookup(geoid)} ${stateNameLookup(state)}`;
  }
  const geonameMod = geoname.replace(/\s/g, '-');

  return geonameMod;
}
