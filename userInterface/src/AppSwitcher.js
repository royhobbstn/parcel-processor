// @ts-check

import React, { useState, useEffect } from 'react';
import DPApp from './srcDerivedProducts/App';
import EnvBar from './EnvBar';
import SqsQueuesApp from './srcSendSQS/App';
import InboxApp from './srcInbox/App';
import DlqReplayApp from './srcDlqReplay/App';

import { Button } from 'semantic-ui-react';

function AppSwitcher() {
  const [env, updateEnv] = useState('down');
  const [app, updateApp] = useState('');

  useEffect(() => {
    let time;
    window.onload = resetTimer;
    document.onmousemove = resetTimer;
    document.onclick = resetTimer;

    function logout() {
      updateEnv('down');
      updateApp('');
    }

    function resetTimer() {
      clearTimeout(time);
      time = setTimeout(logout, 1000 * 60 * 5);
    }

    // load possible sub-geographies from a given parent geography
    fetch(`http://localhost:4000/fetchEnv`)
      .then(res => res.json())
      .then(res => {
        console.log(res);
        updateEnv(res.env);
      })
      .catch(err => {
        console.error(err);
      });
  }, []);

  return (
    <div style={{ padding: '20px' }}>
      <EnvBar env={env} />
      {!app ? (
        <div style={{ margin: 'auto', width: '100px' }}>
          <br />
          <br />
          <Button style={{ width: '200px' }} onClick={() => updateApp('inbox')}>
            Inbox
          </Button>
          <br />
          <br />
          <Button style={{ width: '200px' }} onClick={() => updateApp('derived-product')}>
            Derived Product
          </Button>
          <br />
          <br />
          <Button style={{ width: '200px' }} onClick={() => updateApp('sqs')}>
            Send SQS Message
          </Button>
          <br />
          <br />
          <Button style={{ width: '200px' }} onClick={() => updateApp('dlq')}>
            Replay DLQ Message
          </Button>
        </div>
      ) : null}

      {app === 'inbox' ? <InboxApp env={env} /> : null}
      {app === 'derived-product' ? <DPApp env={env} /> : null}
      {app === 'sqs' ? <SqsQueuesApp env={env} /> : null}
      {app === 'dlq' ? <DlqReplayApp env={env} /> : null}
    </div>
  );
}

export default AppSwitcher;
