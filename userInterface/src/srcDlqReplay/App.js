// @ts-check

import React, { useState } from 'react';
import { Dropdown, Table, Button } from 'semantic-ui-react';
import { sqsDlqQueues } from '../srcInbox/dropdownValues';

function SendSQS({ env }) {
  const [queueSelection, updateQueueSelection] = useState('');
  const [messages, updateMessages] = useState([]);
  const [logText, updateLogText] = useState('');

  const handleSubmit = (evt, data) => {
    const fetchUrl = `http://localhost:4000/${queueSelection}`;

    fetch(fetchUrl)
      .then(response => response.json())
      .then(response => {
        updateMessages(response.messages);
        updateLogText('');
      })
      .catch(err => {
        console.log(err);
        alert('Problem sending inbox data to SQS.');
      });
  };

  const deleteMessage = message => {
    console.log(message);

    const deletePayload = {
      ReceiptHandle: message.ReceiptHandle,
    };

    const fetchUrl = `http://localhost:4000/delete/${queueSelection}`;

    fetch(fetchUrl, {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      method: 'POST',
      body: JSON.stringify(deletePayload),
    })
      .then(response => response.json())
      .then(response => {
        alert('Delete request sent successfully');
        updateMessages([]);
        updateLogText('');
      })
      .catch(err => {
        console.log(err);
        alert('Problem sending delete request.');
      });
  };

  const replayMessage = message => {
    console.log(message);

    const replayPayload = {
      ReceiptHandle: message.ReceiptHandle,
      Body: message.Body,
    };

    const fetchUrl = `http://localhost:4000/replay/${queueSelection}`;

    fetch(fetchUrl, {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      method: 'POST',
      body: JSON.stringify(replayPayload),
    })
      .then(response => response.json())
      .then(response => {
        alert('Message re-created in Original Queue and deleted from DLQ');
        updateMessages([]);
        updateLogText('');
      })
      .catch(err => {
        console.log(err);
        alert('Problem sending replay request.');
      });
  };

  const loadLogData = messageId => {
    let type;
    if (queueSelection === 'viewInboxDlq') {
      type = 'inbox';
    } else if (queueSelection === 'viewSortDlq') {
      type = 'sort';
    } else if (queueSelection === 'viewProductDlq') {
      type = 'product';
    } else {
      console.error('Unexpected queueSelection: ' + queueSelection);
    }

    fetch(`http://localhost:4000/getLogfile?messageId=${messageId}&messageType=${type}`)
      .then(response => response.text())
      .then(response => {
        console.log(response);
        updateLogText(response);
      })
      .catch(err => {
        console.log(err);
        alert('Unable to find logfile.');
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
        options={sqsDlqQueues}
        value={queueSelection}
        onChange={(evt, data) => {
          // @ts-ignore
          updateQueueSelection(data.value);
        }}
      />
      <Button
        style={{ width: '120px', marginLeft: '40px' }}
        disabled={!queueSelection}
        onClick={handleSubmit}
      >
        Read Next
      </Button>
      <br />
      <br />
      <br />
      <p>Found Message:</p>
      <div style={{ maxHeight: '250px', overflowY: 'scroll' }}>
        {messages.length ? (
          <Table selectable celled size="small">
            <Table.Body>
              {messages.map(source => {
                return (
                  <Table.Row
                    key={source.MessageId}
                    onDoubleClick={() => {
                      loadLogData(source.MessageId);
                    }}
                  >
                    <Table.Cell style={{ width: '160px' }}>{source.MessageId}</Table.Cell>
                    <Table.Cell>{source.Body}</Table.Cell>
                    <Table.Cell style={{ width: '120px' }}>
                      <Button
                        style={{ width: '100px', margin: 'auto' }}
                        onClick={() => {
                          replayMessage(source);
                        }}
                      >
                        Replay
                      </Button>
                    </Table.Cell>
                    <Table.Cell style={{ width: '120px' }}>
                      <Button
                        style={{ width: '100px', margin: 'auto' }}
                        onClick={() => {
                          deleteMessage(source);
                        }}
                      >
                        Delete
                      </Button>
                    </Table.Cell>
                  </Table.Row>
                );
              })}
            </Table.Body>
          </Table>
        ) : (
          <p>No messages found.</p>
        )}
      </div>
      <br />
      <pre
        style={{
          height: logText ? '230px' : '0',
          overflowY: 'scroll',
          border: '1px solid grey',
          whiteSpace: 'pre-wrap',
          wordWrap: 'break-word',
        }}
      >
        {logText}
      </pre>
    </div>
  );
}

export default SendSQS;
