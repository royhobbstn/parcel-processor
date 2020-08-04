// @ts-check

import React, { useState } from 'react';
import { Dropdown, TextArea, Button, Grid, Input } from 'semantic-ui-react';
import { sqsQueues, queueEnvironments, messageTypeOptions } from '../srcInbox/dropdownValues';

function SendSQS({ env }) {
  const [payload, updatePayload] = useState('{"dryRun": true}');
  const [queueSelection, updateQueueSelection] = useState('');
  const [queueEnv, updateQueueEnv] = useState('');
  const [messageType, updateMessageType] = useState('');
  const [geodStartsWith, updateGeoidStartsWith] = useState('');

  const submitGeoidQuery = () => {};

  const handleSubmit = (evt, data) => {
    const fetchUrl = `http://localhost:4000/${queueSelection}?env=${queueEnv}`;

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
        alert('Successfully sent payload to SQS.');
      })
      .catch(err => {
        console.log(err);
        alert('Problem sending payload to SQS.');
      });
  };

  return (
    <div>
      <Grid columns={2} divided>
        <Grid.Row>
          <Grid.Column>
            <br />
            <Dropdown
              style={{ border: '1px solid', width: '75%' }}
              placeholder="Environment"
              selection
              options={queueEnvironments}
              value={queueEnv}
              onChange={(evt, data) => {
                // @ts-ignore
                updateQueueEnv(data.value);
              }}
            />
            <br />
            <br />
            <Dropdown
              style={{ border: '1px solid', width: '75%' }}
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
              style={{
                border: '1px solid',
                width: '90%',
                fontFamily: 'monospace',
                minHeight: '250px',
              }}
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
                disabled={!(payload && queueSelection && queueEnv)}
                onClick={handleSubmit}
              >
                Create SQS Message
              </Button>
            </div>
          </Grid.Column>
          <Grid.Column>
            <br />
            <Dropdown
              style={{ border: '1px solid', width: '75%' }}
              placeholder="Message Type"
              selection
              options={messageTypeOptions}
              value={messageType}
              onChange={(evt, data) => {
                // @ts-ignore
                updateMessageType(data.value);
              }}
            />
            <br />
            <br />
            <Input
              style={{
                width: '75%',
              }}
              action={{
                icon: 'play',
                onClick: submitGeoidQuery,
              }}
              placeholder="Geoid Starts With (optional)"
              value={geodStartsWith}
              onChange={(evt, data) => {
                updateGeoidStartsWith(data.value);
              }}
            />
            <br />
            <br />
            <br />
          </Grid.Column>
        </Grid.Row>
      </Grid>
    </div>
  );
}

export default SendSQS;
