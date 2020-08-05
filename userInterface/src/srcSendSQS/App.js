// @ts-check

import React, { useState } from 'react';
import { Dropdown, TextArea, Button, Grid, Input, Table, Icon, Modal } from 'semantic-ui-react';

import { sqsQueues, queueEnvironments, messageTypeOptions } from '../srcInbox/dropdownValues';

function SendSQS({ env }) {
  const [payload, updatePayload] = useState('{"dryRun": true}');
  const [queueSelection, updateQueueSelection] = useState('');
  const [queueEnv, updateQueueEnv] = useState('');
  const [messageType, updateMessageType] = useState('');
  const [geodStartsWith, updateGeoidStartsWith] = useState('');
  const [tableResults, updateTableResults] = useState([]);

  const [messageBody, updateMessageBody] = useState('');
  const [showMessageModal, updateShowMessageModal] = useState(false);

  const [logText, updateLogText] = useState('');
  const [showLogModal, updateShowLogModal] = useState(false);

  const queryLogfile = (messageId, messageType) => {
    fetch(`http://localhost:4000/getLogfile?messageId=${messageId}&messageType=${messageType}`)
      .then(response => response.text())
      .then(response => {
        updateLogText(response);
      })
      .catch(err => {
        console.log(err);
        alert('Unable to find logfile.');
      });
  };

  const submitGeoidQuery = () => {
    if (!messageType) {
      window.alert('Message Type Required');
      return;
    }

    const fetchUrl = `http://localhost:4000/getSQSMessageBody?type=${messageType}&geoid=${geodStartsWith}`;

    fetch(fetchUrl, {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      method: 'GET',
    })
      .then(response => response.json())
      .then(res => {
        updateTableResults(res);
      })
      .catch(err => {
        console.log(err);
        updateTableResults([]);
        alert('Error sending Query.');
      });
  };

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
            <div
              style={{
                height: '360px',
                border: '1px dotted grey',
                overflowY: 'scroll',
                marginTop: '10px',
              }}
            >
              <Table>
                <Table.Header>
                  <Table.Row>
                    <Table.HeaderCell>Created</Table.HeaderCell>
                    <Table.HeaderCell>SQS</Table.HeaderCell>
                    <Table.HeaderCell>Log</Table.HeaderCell>
                    <Table.HeaderCell>Geoid</Table.HeaderCell>
                  </Table.Row>
                </Table.Header>
                <Table.Body>
                  {tableResults.map(result => {
                    return (
                      <Table.Row
                        key={result.created}
                        onDoubleClick={() => {
                          updatePayload(result.message_body);
                        }}
                      >
                        <Table.Cell>{result.created}</Table.Cell>
                        <Table.Cell>
                          <Icon
                            style={{ cursor: 'pointer' }}
                            onClick={() => {
                              updateMessageBody(result.message_body);
                              updateShowMessageModal(true);
                            }}
                            name="sticky note"
                          />
                        </Table.Cell>
                        <Table.Cell>
                          <Icon
                            style={{ cursor: 'pointer' }}
                            onClick={() => {
                              queryLogfile(result.message_id, result.message_type);
                              updateShowLogModal(true);
                            }}
                            name="list alternate outline"
                          />
                        </Table.Cell>
                        <Table.Cell>{result.geoid}</Table.Cell>
                      </Table.Row>
                    );
                  })}
                </Table.Body>
              </Table>
            </div>
          </Grid.Column>
        </Grid.Row>
      </Grid>
      <Modal open={showMessageModal}>
        <Modal.Header>SQS Message</Modal.Header>
        <div style={{ maxWidth: '800px', maxHeight: '450px', overflowY: 'scroll', margin: 'auto' }}>
          <Modal.Content>
            <pre style={{ margin: '20px', whiteSpace: 'pre-wrap' }}>
              {JSON.stringify(JSON.parse(messageBody || '{}'), null, '  ')}
            </pre>
          </Modal.Content>
        </div>
        <Modal.Actions>
          <Button
            secondary
            onClick={() => {
              updateShowMessageModal(false);
              updateMessageBody('');
            }}
          >
            Close
          </Button>
        </Modal.Actions>
      </Modal>
      <Modal open={showLogModal}>
        <Modal.Header>Process Logs</Modal.Header>
        <div style={{ maxWidth: '800px', maxHeight: '450px', overflowY: 'scroll', margin: 'auto' }}>
          <Modal.Content>
            <Table style={{ fontFamily: 'monospace', fontSize: '11px' }}>
              <Table.Header>
                <Table.Row>
                  <Table.HeaderCell>Line</Table.HeaderCell>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {logText.split('\n').map((line, index) => {
                  return (
                    <Table.Row key={index}>
                      <Table.Cell>{line}</Table.Cell>
                    </Table.Row>
                  );
                })}
              </Table.Body>
            </Table>
          </Modal.Content>
        </div>
        <Modal.Actions>
          <Button
            secondary
            onClick={() => {
              updateShowLogModal(false);
            }}
          >
            Close
          </Button>
        </Modal.Actions>
      </Modal>
    </div>
  );
}

export default SendSQS;
