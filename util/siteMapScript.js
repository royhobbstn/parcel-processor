const fs = require('fs');

exports.siteMap = siteMap;

async function siteMap(ctx) {
  const currentIsoDate = new Date().toISOString().slice(0, 10);

  // high priority the coverage map, the main page 1

  const top = '<?xml version="1.0" encoding="UTF-8"?>\n';

  const urlsetTop = '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n';

  const homepage = `<url>
  <loc>https://www.parcel-map.com/</loc>
  <lastmod>${currentIsoDate}</lastmod>
  <changefreq>weekly</changefreq>
  <priority>1</priority>
</url>\n`;

  const coverageMap = `<url>
  <loc>https://www.parcel-map.com/coverage-map</loc>
  <lastmod>${currentIsoDate}</lastmod>
  <changefreq>weekly</changefreq>
  <priority>1</priority>
</url>\n`;

  const maps = [];

  // loop

  // create map site
  const map = `<url>
  <loc>https://www.parcel-map.com/coverage-map</loc>
  <lastmod>TODO</lastmod>
  <changefreq>monthly</changefreq>
  <priority>0.1</priority>
</url>\n`;

  // push map site
  maps.push(map);

  // end loop

  const urlsetBottom = `</urlset>\n`;

  const xml = top + urlsetTop + homepage + coverageMap + maps.join() + urlsetBottom;

  fs.writeFileSync('sitemap.xml', xml, 'utf8');

  // all the map pages 0.1
}

// todo  robots.txt

// need to ping google when sitemap has changed
// - dont do this yet

siteMap();
