import React, { useState, useEffect } from 'react';
import { Card } from 'semantic-ui-react';

function StatsCard({ selectedDownload }) {
  const [statsInfo, updateStatsInfo] = useState({});

  useEffect(() => {
    fetch(
      `http://localhost:4000/proxyStatFile?key=${encodeURIComponent(selectedDownload.product_key)}`,
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
    <Card style={{ width: '100%' }}>
      <Card.Content>
        <Card.Header>{`${selectedDownload.geoid}, ${selectedDownload.geoname}`}</Card.Header>
        <Card.Meta>{selectedDownload.source_name}</Card.Meta>
        <Card.Meta>
          {`${selectedDownload.last_checked}     #${selectedDownload.download_ref}`}
        </Card.Meta>
        <Card.Meta>{selectedDownload.original_filename}</Card.Meta>
        <Card.Description>STATS HERE</Card.Description>
      </Card.Content>
    </Card>
  );
}

export default StatsCard;
