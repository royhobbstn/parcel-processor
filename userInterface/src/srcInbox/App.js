// @ts-check

import React, { useState } from 'react';
import { Dropdown, Input, Checkbox, Button, Table } from 'semantic-ui-react';
import { summaryLevels } from './dropdownValues.js';

function App({ env }) {
  const [urlKeyVal, updateUrlKeyVal] = useState('');
  const [urlKeyStatus, updateUrlKeyStatus] = useState(false);
  const [sumlevStatus, updateSumlevStatus] = useState(false);
  const [sumlevVal, updateSumlevVal] = useState('');
  const [geoidVal, updateGeoidVal] = useState('');
  const [geoidStatus, updateGeoidStatus] = useState(false);
  const [dryRun, updateDryRun] = useState(false);
  const [sourceVal, updateSourceVal] = useState('');
  const [sourceStatus, updateSourceStatus] = useState(false);
  const [sourceList, updateSourceList] = useState(null);

  const handleSubmit = async (evt, data) => {
    // sendInboxSQS

    const isWebpage =
      sourceVal.includes('http://') ||
      sourceVal.includes('https://') ||
      sourceVal.includes('ftp://') ||
      sourceVal.includes('ftps://');

    const isEmail = sourceVal.includes('@');

    if (!isWebpage && !isEmail) {
      alert('unable to determine sourceType (webpage|email).  SQS Message not sent.');
      return;
    }

    const sourceType = isWebpage ? 'webpage' : isEmail ? 'email' : 'unknown';

    const STATEFIPS = geoidVal.slice(0, 2);

    const COUNTYFIPS = sumlevVal === '050' ? geoidVal.slice(2) : sumlevVal === '040' ? '000' : '';

    const PLACEFIPS = sumlevVal === '160' ? geoidVal.slice(2) : '';

    const sourceExists = await checkSourceExists(sourceVal);

    if (!sourceExists) {
      const result = window.confirm('This will create a new source.  Is that okay?');
      if (!result) {
        console.log('Operation cancelled');
        return;
      }
    }

    const payload = {
      sourceType,
      urlKeyVal,
      sumlevVal,
      geoidVal,
      sourceVal,
      dryRun,
      STATEFIPS,
      COUNTYFIPS,
      PLACEFIPS,
    };

    fetch(`http://localhost:4000/sendInboxSQS`, {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      method: 'POST',
      body: JSON.stringify(payload),
    })
      .then(response => response.json())
      .then(() => {
        alert('Successfully sent inbox data to SQS.');
      })
      .catch(err => {
        console.log(err);
        alert('Problem sending inbox data to SQS.');
      });
  };

  const handleUrlKeyClick = (evt, data) => {
    window
      .fetch(`http://localhost:4000/proxyHeadRequest?url=${window.encodeURIComponent(urlKeyVal)}`)
      .then(res => res.json())
      .then(data => {
        updateUrlKeyStatus(data.status);
      })
      .catch(() => {
        alert('error making request');
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
          console.log(data);
          // @ts-ignore
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
        placeholder="Enter the file Download URL"
        value={urlKeyVal}
        onChange={(evt, data) => {
          updateUrlKeyStatus(false);
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
        placeholder="Working Full Url of Landing Page (not file download link) or Email Address"
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
          } else {
            updateSourceStatus(false);
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

function checkSourceExists(sourceName) {
  return window
    .fetch(
      `http://localhost:4000/checkSourceExists?sourceName=${window.encodeURIComponent(sourceName)}`,
    )
    .then(res => res.json())
    .then(data => {
      return data.status;
    });
}
