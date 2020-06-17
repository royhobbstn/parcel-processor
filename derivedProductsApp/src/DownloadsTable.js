import React from 'react';
import { Table } from 'semantic-ui-react';

function DownloadsTable({ statFiles, selectedDownload, updateSelectedDownload }) {
  return (
    <Table
      selectable
      celled
      size="small"
      style={{
        minHeight: '60px',
        maxHeight: '150px',
        overflowY: 'scroll',
        border: '1px dotted grey',
      }}
    >
      <Table.Header>
        <Table.Row>
          <Table.HeaderCell>GeoName</Table.HeaderCell>
          <Table.HeaderCell>GeoID</Table.HeaderCell>
          <Table.HeaderCell>Source</Table.HeaderCell>
          <Table.HeaderCell>Timestamp</Table.HeaderCell>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {statFiles.length ? (
          statFiles.map(d => {
            return (
              <Table.Row
                key={d.download_id}
                onClick={() => {
                  if (d.download_id === (selectedDownload && selectedDownload.download_id)) {
                    updateSelectedDownload(null);
                  } else {
                    updateSelectedDownload(d);
                  }
                }}
              >
                <Table.Cell>{d.geoname}</Table.Cell>
                <Table.Cell>{d.geoid}</Table.Cell>
                <Table.Cell>{d.source_name}</Table.Cell>
                <Table.Cell>{d.last_checked}</Table.Cell>
              </Table.Row>
            );
          })
        ) : (
          <Table.Row key={1}>
            <Table.Cell>No Results</Table.Cell>
          </Table.Row>
        )}
      </Table.Body>
    </Table>
  );
}

export default DownloadsTable;
