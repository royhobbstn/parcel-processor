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

import { Button, Grid } from 'semantic-ui-react';

let time;

function AppSwitcher() {
  const [env, updateEnv] = useState('down');
  const [app, updateApp] = useState('');
  const [online, updateOnline] = useState('no'); // yes, no, trying
  const [title, updateTitle] = useState('Home');
  const [loadingInventory, updateLoadingInventory] = useState(false);

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

  const triggerSiteData = () => {
    return fetch('http://localhost:4000/triggerSiteData');
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
      <EnvBar
        env={env}
        updateApp={updateApp}
        online={online}
        title={title}
        updateTitle={updateTitle}
      />
      {!app ? (
        <div>
          <Grid columns={2} divided>
            <Grid.Row>
              <Grid.Column>
                <div style={{ margin: 'auto', width: '200px' }}>
                  <br />
                  <br />
                  <Button
                    style={{ width: '200px' }}
                    onClick={() => {
                      updateTitle('Inbox');
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
                      updateTitle('Sort');
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
                      updateTitle('Send SQS');
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
                      updateTitle('Replay DLQ');
                      updateApp('dlq');
                    }}
                  >
                    Replay DLQ Message
                  </Button>
                  <br />
                </div>
              </Grid.Column>
              <Grid.Column>
                <div style={{ margin: 'auto', width: '200px' }}>
                  <br />
                  <br />
                  <Button
                    style={{ width: '200px' }}
                    onClick={() => {
                      updateTitle('View Logs');
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
                      updateTitle('Launch Workers');
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
                      updateTitle('Delete Records');
                      updateApp('delete');
                    }}
                  >
                    Delete
                  </Button>
                  <br />
                  <br />
                  <Button
                    style={{ width: '200px' }}
                    onClick={async () => {
                      updateLoadingInventory(true);
                      triggerSiteData()
                        .then(() => {
                          window.alert('Updated Successfully!');
                        })
                        .catch(() => {
                          window.alert('There was a problem updating Site Data');
                        })
                        .finally(() => {
                          updateLoadingInventory(false);
                        });
                    }}
                    loading={loadingInventory}
                  >
                    Update Inventory
                  </Button>
                </div>
              </Grid.Column>
            </Grid.Row>
          </Grid>
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
