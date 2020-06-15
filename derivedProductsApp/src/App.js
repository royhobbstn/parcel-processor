import React, { useState } from 'react';
import { Input, Label, Table, Card } from 'semantic-ui-react';

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
                updateStatFiles(data);
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
            <Table.HeaderCell>FIPS</Table.HeaderCell>
            <Table.HeaderCell>Source</Table.HeaderCell>
            <Table.HeaderCell>Timestamp</Table.HeaderCell>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {statFiles.length ? (
            statFiles.map(d => {
              return (
                <Table.Row
                  key={d.downloadId}
                  onClick={() => {
                    console.log({ dl1: d.downloadId, dl2: selectedDownload });
                    if (d.downloadId === (selectedDownload && selectedDownload.downloadId)) {
                      updateSelectedDownload(null);
                    } else {
                      updateSelectedDownload(d);
                    }
                  }}
                >
                  <Table.Cell>{d.geoname}</Table.Cell>
                  <Table.Cell>{d.fips}</Table.Cell>
                  <Table.Cell>{d.sourceName}</Table.Cell>
                  <Table.Cell>{d.downloadDate}</Table.Cell>
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
      <span>{JSON.stringify(statFiles)}</span>
      {selectedDownload ? (
        <Card>
          <Card.Content>
            <Card.Header>Steve Sanders</Card.Header>
            <Card.Meta>Friends of Elliot</Card.Meta>
            <Card.Description>
              Steve wants to add you to the group <strong>best friends</strong>
            </Card.Description>
          </Card.Content>
          <Card.Content extra>{selectedDownload.downloadId}</Card.Content>
        </Card>
      ) : (
        <span />
      )}
    </div>
  );
}

export default App;
