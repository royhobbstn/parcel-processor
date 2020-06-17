import React from 'react';
import { Card } from 'semantic-ui-react';

function StatsCard({ statsInfo, selectedDownload }) {
  return (
    <Card style={{ width: '100%' }}>
      <Card.Content>
        <Card.Header>{`${selectedDownload.geoid}, ${selectedDownload.geoname}`}</Card.Header>
        <Card.Meta>{selectedDownload.source_name}</Card.Meta>
        <Card.Meta>Downloaded: {selectedDownload.last_checked}</Card.Meta>
        <Card.Meta>Download Ref: {selectedDownload.download_ref}</Card.Meta>
        <Card.Meta>Original Filename: {selectedDownload.original_filename}</Card.Meta>
        <Card.Description>Records: {statsInfo.rowCount || '...?'} </Card.Description>
      </Card.Content>
    </Card>
  );
}

export default StatsCard;
