// @ts-check

import React, { useState, useEffect } from 'react';
import DPApp from './srcDerivedProducts/App';
import EnvBar from './EnvBar';
import SqsQueuesApp from './srcSendSQS/App';
import InboxApp from './srcInbox/App';
import DlqReplayApp from './srcDlqReplay/App';
import ViewLogsApp from './srcViewLogs/App';
import LaunchApp from './srcLaunch/App';
import DeleteApp from './srcDelete/App';

import { Button } from 'semantic-ui-react';

let time;

function AppSwitcher() {
  const [env, updateEnv] = useState('down');
  const [app, updateApp] = useState('');
  const [online, updateOnline] = useState('no'); // yes, no, trying

  const pingDB = () => {
    updateOnline('trying');
    fetch('http://localhost:4000/acquireConnection')
      .then(res => res.json())
      .then(res => {
        console.log(res);
        updateOnline('yes');
      })
      .catch(err => {
        alert('was not able to connect to the database');
        console.error(err);
        updateOnline('no');
      });
  };

  useEffect(() => {
    // this will be re-triggered with every change to this component

    window.onload = resetTimer;
    document.onmousemove = resetTimer;
    document.onclick = resetTimer;

    const checkEnv = () => {
      if (app !== '') {
        // maintain an active db connection (unless on homepage)
        pingDB();
      }

      fetch(`http://localhost:4000/fetchEnv`)
        .then(res => res.json())
        .then(res => {
          updateEnv(res.env);
        })
        .catch(err => {
          updateEnv('down');
          updateOnline('no');
          console.error(err);
        });
    };

    function resetApp() {
      updateApp('');
      updateOnline('no');
    }

    function resetTimer() {
      clearTimeout(time);
      time = setTimeout(resetApp, 1000 * 60 * 5);
    }

    if (!time) {
      // ping server
      setInterval(checkEnv, 1000 * 60 * 4);

      // initial call
      checkEnv();
    }
  }, [app]);

  return (
    <div style={{ padding: '20px' }}>
      <EnvBar env={env} updateApp={updateApp} online={online} />
      {!app ? (
        <div style={{ margin: 'auto', width: '100px' }}>
          <br />
          <br />
          <Button
            style={{ width: '200px' }}
            onClick={() => {
              updateApp('inbox');
            }}
          >
            Inbox
          </Button>
          <br />
          <br />
          <Button
            style={{ width: '200px' }}
            onClick={() => {
              updateApp('sort');
            }}
          >
            Sort
          </Button>
          <br />
          <br />
          <Button
            style={{ width: '200px' }}
            onClick={() => {
              updateApp('sqs');
            }}
          >
            Send SQS Message
          </Button>
          <br />
          <br />
          <Button
            style={{ width: '200px' }}
            onClick={() => {
              updateApp('dlq');
            }}
          >
            Replay DLQ Message
          </Button>
          <br />
          <br />
          <Button
            style={{ width: '200px' }}
            onClick={() => {
              updateApp('logs');
            }}
          >
            View Logs
          </Button>
          <br />
          <br />
          <Button
            style={{ width: '200px' }}
            onClick={() => {
              updateApp('launch');
            }}
          >
            Launch
          </Button>
          <br />
          <br />
          <Button
            style={{ width: '200px' }}
            onClick={() => {
              updateApp('delete');
            }}
          >
            Delete
          </Button>
        </div>
      ) : null}

      {app === 'inbox' ? <InboxApp env={env} /> : null}
      {app === 'sort' ? <DPApp env={env} /> : null}
      {app === 'sqs' ? <SqsQueuesApp env={env} /> : null}
      {app === 'dlq' ? <DlqReplayApp env={env} /> : null}
      {app === 'logs' ? <ViewLogsApp env={env} /> : null}
      {app === 'launch' ? <LaunchApp env={env} /> : null}
      {app === 'delete' ? <DeleteApp env={env} /> : null}
    </div>
  );
}

export default AppSwitcher;
