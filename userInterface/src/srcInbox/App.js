import React, { useState } from 'react';
import { Dropdown, Input, Checkbox, Button, Table } from 'semantic-ui-react';
import { summaryLevels } from './dropdownValues.js';

function App({ env }) {
  const [urlKeyVal, updateUrlKeyVal] = useState('');
  const [urlKeyStatus, updateUrlKeyStatus] = useState(false);
  const [sumlevStatus, updateSumlevStatus] = useState(false);
  const [sumlevVal, updateSumlevVal] = useState(false);
  const [geoidVal, updateGeoidVal] = useState('');
  const [geoidStatus, updateGeoidStatus] = useState('');
  const [dryRun, updateDryRun] = useState(true);
  const [sourceVal, updateSourceVal] = useState('');
  const [sourceStatus, updateSourceStatus] = useState(false);
  const [sourceList, updateSourceList] = useState(null);

  const handleSubmit = (evt, data) => {
    console.log('click');
  };

  const handleUrlKeyClick = (evt, data) => {
    window
      .fetch(urlKeyVal, { method: 'HEAD', mode: 'cors' })
      .then(() => {
        updateUrlKeyStatus(true);
      })
      .catch(() => {
        updateUrlKeyStatus(false);
      });
  };

  const handleSourceInquiry = (evt, data) => {
    // get a list of webpages or emails from the same domain

    window
      .fetch('http://localhost:4000/querySources?name=' + window.encodeURIComponent(sourceVal))
      .then(res => res.json())
      .then(data => {
        updateSourceList(data);
      })
      .catch(() => {
        alert('Call failed.  Unable to update SourceList.');
        updateSourceList(null);
      });
  };

  return (
    <div>
      <br />
      <Dropdown
        style={{ border: '1px solid', borderColor: sumlevStatus ? 'green' : 'red' }}
        placeholder="Sumlev"
        selection
        options={summaryLevels}
        onChange={(evt, data) => {
          updateSumlevVal(data.value);
          updateSumlevStatus(true);
          updateGeoidVal('');
          updateGeoidStatus(false);
        }}
      />
      <span style={{ paddingLeft: '40px' }} />
      <Input
        style={{ border: '1px solid', borderColor: geoidStatus ? 'green' : 'red' }}
        label="Geoid"
        placeholder="Enter a Geoid"
        value={geoidVal}
        onChange={(evt, data) => {
          updateGeoidVal(data.value);

          // status
          if (sumlevVal === '040') {
            if (data.value.length === 2) {
              updateGeoidStatus(true);
            } else {
              updateGeoidStatus(false);
            }
          } else if (sumlevVal === '050') {
            if (data.value.length === 5) {
              updateGeoidStatus(true);
            } else {
              updateGeoidStatus(false);
            }
          } else if (sumlevVal === '160') {
            if (data.value.length === 7) {
              updateGeoidStatus(true);
            } else {
              updateGeoidStatus(false);
            }
          }
        }}
      />
      <br />
      <br />
      <Input
        style={{ width: '75%', border: '1px solid', borderColor: urlKeyStatus ? 'green' : 'red' }}
        action={{
          icon: 'play',
          onClick: handleUrlKeyClick,
        }}
        label="Url or Key"
        placeholder="Enter an http(s) endpoint or S3 Key"
        value={urlKeyVal}
        onChange={(evt, data) => {
          updateUrlKeyVal(data.value);
        }}
      />
      <br />
      <br />
      <Input
        style={{ width: '75%', border: '1px solid', borderColor: sourceStatus ? 'green' : 'red' }}
        action={{
          icon: 'play',
          onClick: handleSourceInquiry,
        }}
        label="Source Name"
        placeholder="Url or Email Address"
        value={sourceVal}
        onChange={(evt, data) => {
          updateSourceVal(data.value);
          if (
            data.value.includes('@') ||
            data.value.includes('http://') ||
            data.value.includes('https://') ||
            data.value.includes('ftp://') ||
            data.value.includes('ftps://')
          ) {
            updateSourceStatus(true);
          }
        }}
      />
      <br />
      {sourceList ? (
        <div style={{ margin: '30px' }}>
          <p>Source Query Results:</p>
          <div style={{ maxHeight: '200px', overflowY: 'scroll' }}>
            <Table selectable celled size="small">
              <Table.Body>
                {sourceList.map(source => {
                  return (
                    <Table.Row
                      key={source.source_id}
                      onDoubleClick={() => {
                        console.log('doubleclick');
                        updateSourceVal(source.source_name);
                      }}
                    >
                      <Table.Cell>{source.source_id}</Table.Cell>
                      <Table.Cell>{source.source_name}</Table.Cell>
                      <Table.Cell>{source.source_type}</Table.Cell>
                    </Table.Row>
                  );
                })}
              </Table.Body>
            </Table>
          </div>
        </div>
      ) : null}
      <br />
      <Checkbox
        label="Dry Run?"
        onChange={() => {
          updateDryRun(!dryRun);
        }}
        checked={dryRun}
      />
      <br />
      <br />
      <div style={{ width: '200px', margin: 'auto' }}>
        <Button
          style={{ width: '200px', margin: 'auto' }}
          disabled={
            !(
              urlKeyStatus &&
              sumlevStatus &&
              geoidStatus &&
              sourceStatus &&
              urlKeyVal !== 'todo make this blank'
            )
          }
          onClick={handleSubmit}
        >
          Create SQS Message
        </Button>
      </div>
    </div>
  );
}

export default App;
