import React, { useState, useEffect } from 'react';
import { Grid } from 'semantic-ui-react';
import AttributesTable from './AttributesTable';
import AttributesInfoCard from './AttributesInfoCard';
import StatsCard from './StatsCard';

function StatsLayout({ selectedDownload }) {
  const [statsInfo, updateStatsInfo] = useState({});
  const [selectedFieldKey, updateSelectedFieldKey] = useState(null);

  useEffect(() => {
    // modify key to point to -stat.json rather than .ndgeojson
    const modKey = selectedDownload.product_key.replace('.ndgeojson', '-stat.json');
    fetch(
      `http://localhost:4000/proxyS3File?bucket=${encodeURIComponent(
        'data-products-po',
      )}&key=${encodeURIComponent(modKey)}`,
    )
      .then(res => res.json())
      .then(res => {
        console.log(res);
        updateStatsInfo(res);
      })
      .catch(err => {
        console.error(err);
      });
  }, [selectedDownload]);

  return (
    <div>
      <StatsCard statsInfo={statsInfo} selectedDownload={selectedDownload} />
      <Grid columns={2} divided>
        <Grid.Row>
          <Grid.Column>
            <AttributesTable
              statsInfo={statsInfo}
              selectedFieldKey={selectedFieldKey}
              updateSelectedFieldKey={updateSelectedFieldKey}
            />
          </Grid.Column>
          <Grid.Column>
            {selectedFieldKey ? (
              <AttributesInfoCard statsInfo={statsInfo} selectedFieldKey={selectedFieldKey} />
            ) : (
              <span></span>
            )}
          </Grid.Column>
        </Grid.Row>
      </Grid>
    </div>
  );
}

export default StatsLayout;
