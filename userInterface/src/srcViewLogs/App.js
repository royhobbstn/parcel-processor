// @ts-check

import React, { useState } from 'react';
import { Form, Radio, Grid, Input, Table, Button } from 'semantic-ui-react';

function ViewLogs({ env }) {
  const [radioSelection, updateRadioSelection] = useState('type');
  const [refVal, updateRefVal] = useState('');
  const [geoidVal, updateGeoidVal] = useState('');
  const [records, updateRecords] = useState([]);
  const [logText, updateLogText] = useState('');

  const searchLogsByType = type => {
    const fetchUrl = `http://localhost:4000/searchLogsByType?type=${type}`;

    fetch(fetchUrl)
      .then(response => response.json())
      .then(response => {
        updateRecords(response);
        updateLogText('');
      })
      .catch(err => {
        console.log(err);
        alert('Problem querying database for logs.');
      });
  };

  const searchLogsByReference = () => {
    const fetchUrl = `http://localhost:4000/searchLogsByReference?ref=${refVal}`;

    fetch(fetchUrl)
      .then(response => response.json())
      .then(response => {
        console.log(response);
        updateRecords(response);
        updateLogText('');
      })
      .catch(err => {
        console.log(err);
        alert('Problem querying database for logs.');
      });
  };

  const searchLogsByGeoid = () => {
    const fetchUrl = `http://localhost:4000/searchLogsByGeoid?geoid=${geoidVal}`;

    fetch(fetchUrl)
      .then(response => response.json())
      .then(response => {
        console.log(response);
        updateRecords(response);
        updateLogText('');
      })
      .catch(err => {
        console.log(err);
        alert('Problem querying database for logs.');
      });
  };

  const loadLogData = record => {
    fetch(
      `http://localhost:4000/getLogfile?messageId=${record.message_id}&messageType=${record.message_type}`,
    )
      .then(response => response.text())
      .then(response => {
        updateLogText(response);
      })
      .catch(err => {
        console.log(err);
        alert('Unable to find logfile.');
      });
  };

  return (
    <div>
      <Grid columns={2} divided>
        <Grid.Row>
          <Grid.Column>
            <div
              style={{
                borderRadius: '5px',
                backgroundColor: '#e0e1e2',
                padding: '20px 40px',
                margin: 'auto',
                width: 'auto',
              }}
            >
              <Form>
                <Form.Field>
                  <b>Select a Search Criteria:</b>
                </Form.Field>
                <Form.Field>
                  <Radio
                    label="By Type"
                    name="radioGroup"
                    value="type"
                    checked={radioSelection === 'type'}
                    onChange={() => updateRadioSelection('type')}
                  />
                </Form.Field>
                <Form.Field>
                  <Radio
                    label="By Ref"
                    name="radioGroup"
                    value="ref"
                    checked={radioSelection === 'ref'}
                    onChange={() => updateRadioSelection('ref')}
                  />
                </Form.Field>
                <Form.Field>
                  <Radio
                    label="By Geoid"
                    name="radioGroup"
                    value="geoid"
                    checked={radioSelection === 'geoid'}
                    onChange={() => updateRadioSelection('geoid')}
                  />
                </Form.Field>
              </Form>
            </div>
          </Grid.Column>
          <Grid.Column>
            <div style={{ width: '100%' }}>
              {radioSelection === 'type' ? (
                <div style={{ margin: 'auto', width: 'auto' }}>
                  <b style={{ display: 'block', paddingLeft: '10px', paddingTop: '10px' }}>
                    Select a Log Type:
                  </b>
                  <Button
                    style={{ width: '100px', margin: '10px' }}
                    onClick={() => searchLogsByType('all')}
                  >
                    All
                  </Button>
                  <Button
                    style={{ width: '100px', margin: '10px' }}
                    onClick={() => searchLogsByType('inbox')}
                  >
                    Inbox
                  </Button>
                  <Button
                    style={{ width: '100px', margin: '10px' }}
                    onClick={() => searchLogsByType('sort')}
                  >
                    Sort
                  </Button>
                  <Button
                    style={{ width: '100px', margin: '10px' }}
                    onClick={() => searchLogsByType('product')}
                  >
                    Product
                  </Button>
                </div>
              ) : null}
              {radioSelection === 'ref' ? (
                <div>
                  <b style={{ display: 'block', padding: '10px' }}>Enter a Reference Number:</b>
                  <Input
                    style={{ width: '250px', padding: '10px', display: 'block' }}
                    placeholder="Reference Number"
                    focus
                    value={refVal}
                    onChange={(evt, data) => {
                      updateRefVal(data.value);
                    }}
                  />
                  <Button
                    style={{ width: '100px', margin: '10px' }}
                    onClick={() => searchLogsByReference()}
                  >
                    Search
                  </Button>
                </div>
              ) : null}
              {radioSelection === 'geoid' ? (
                <div>
                  <b style={{ display: 'block', padding: '10px' }}>Enter a Geoid:</b>
                  <Input
                    style={{ width: '250px', padding: '10px', display: 'block' }}
                    placeholder="Geoid"
                    focus
                    value={geoidVal}
                    onChange={(evt, data) => {
                      updateGeoidVal(data.value);
                    }}
                  />
                  <Button
                    style={{ width: '100px', margin: '10px' }}
                    onClick={() => searchLogsByGeoid()}
                  >
                    Search
                  </Button>
                </div>
              ) : null}
            </div>
          </Grid.Column>
        </Grid.Row>
      </Grid>
      <div
        style={{
          height: records.length ? '230px' : '60px',
          overflowY: 'scroll',
          border: '1px solid grey',
          marginTop: '20px',
        }}
      >
        {records.length ? (
          <Table selectable celled size="small">
            <Table.Header>
              <Table.Row>
                <Table.HeaderCell>Created</Table.HeaderCell>
                <Table.HeaderCell>M.Type</Table.HeaderCell>
                <Table.HeaderCell>Body</Table.HeaderCell>
                <Table.HeaderCell>Geoid</Table.HeaderCell>
                <Table.HeaderCell>P.Type</Table.HeaderCell>
                <Table.HeaderCell>Origin</Table.HeaderCell>
                <Table.HeaderCell>M.Id</Table.HeaderCell>
                <Table.HeaderCell>Ind.Ref</Table.HeaderCell>
                <Table.HeaderCell>P.Ref</Table.HeaderCell>
                <Table.HeaderCell>Dl.Ref</Table.HeaderCell>
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {records.map(record => {
                return (
                  <Table.Row
                    key={record.individual_ref}
                    onDoubleClick={() => {
                      loadLogData(record);
                    }}
                  >
                    <Table.Cell>{record.created}</Table.Cell>
                    <Table.Cell>{record.message_type}</Table.Cell>
                    <Table.Cell>{record.message_body}</Table.Cell>
                    <Table.Cell>{record.geoid}</Table.Cell>
                    <Table.Cell>{record.product_type}</Table.Cell>
                    <Table.Cell>{record.product_origin}</Table.Cell>
                    <Table.Cell>{record.message_id}</Table.Cell>
                    <Table.Cell>{record.individual_ref}</Table.Cell>
                    <Table.Cell>{record.product_ref}</Table.Cell>
                    <Table.Cell>{record.download_ref}</Table.Cell>
                  </Table.Row>
                );
              })}
            </Table.Body>
          </Table>
        ) : (
          <p style={{ padding: '20px 25px' }}>No records</p>
        )}
      </div>
      <pre
        style={{
          height: logText ? '230px' : '60px',
          overflowY: 'scroll',
          border: '1px solid grey',
          whiteSpace: 'pre-wrap',
          wordWrap: 'break-word',
        }}
      >
        {logText ? logText : '\n   No logfile'}
      </pre>
    </div>
  );
}

export default ViewLogs;
