// @ts-check

import React, { useState } from 'react';
import { Dropdown, TextArea, Button } from 'semantic-ui-react';
import { sqsQueues } from '../srcInbox/dropdownValues';

function SendSQS({ env }) {
  const [payload, updatePayload] = useState('{"dryRun": true}');
  const [queueSelection, updateQueueSelection] = useState('');

  const handleSubmit = (evt, data) => {
    const fetchUrl = `http://localhost:4000/${queueSelection}`;

    fetch(fetchUrl, {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      method: 'POST',
      body: payload,
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

  return (
    <div>
      <br />
      <br />
      <Dropdown
        style={{ border: '1px solid', width: '50%' }}
        placeholder="Queue"
        selection
        options={sqsQueues}
        value={queueSelection}
        onChange={(evt, data) => {
          // @ts-ignore
          updateQueueSelection(data.value);
        }}
      />
      <br />
      <br />
      <br />
      <TextArea
        spellCheck="false"
        style={{ border: '1px solid', width: '75%', fontFamily: 'monospace', minHeight: '250px' }}
        label="Payload"
        value={payload}
        onChange={(evt, data) => {
          console.log(data);
          // @ts-ignore
          updatePayload(data.value);
        }}
      />
      <br />
      <br />
      <br />
      <div style={{ width: '200px', margin: 'auto' }}>
        <Button
          style={{ width: '200px', margin: 'auto' }}
          disabled={!(payload && queueSelection)}
          onClick={handleSubmit}
        >
          Create SQS Message
        </Button>
      </div>
    </div>
  );
}

export default SendSQS;
