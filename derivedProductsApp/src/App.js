import React, { useState } from 'react';
import { Input, Label, Table } from 'semantic-ui-react';
import StatsCard from './StatsCard';

function App() {
  const [inputVal, updateInputVal] = useState('');
  const [statFiles, updateStatFiles] = useState([]);
  const [selectedDownload, updateSelectedDownload] = useState(null);

  return (
    <div style={{ padding: '20px' }}>
      <Label pointing="right">
        Enter FIPS or leave blank for all downloads missing derived products
      </Label>
      <Input
        style={{ padding: '10px' }}
        action={{
          icon: 'play',
          onClick: (evt, data) => {
            window
              .fetch('http://localhost:4000/queryStatFiles?fips=' + inputVal)
              .then(res => res.json())
              .then(data => {
                updateStatFiles(data.rows);
              })
              .catch(err => {
                console.error('err:', err);
              });
          },
        }}
        placeholder="Search..."
        value={inputVal}
        onChange={(evt, data) => {
          updateInputVal(data.value);
        }}
      />
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
      <br />
      {selectedDownload ? <StatsCard selectedDownload={selectedDownload} /> : <span />}
    </div>
  );
}

export default App;
