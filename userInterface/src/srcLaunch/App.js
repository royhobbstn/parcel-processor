// @ts-check

import React, { useState, useEffect } from 'react';
import { Grid, Table, Button, Icon, Popup } from 'semantic-ui-react';

function Launch({ env }) {
  const fill = {
    available: 0,
    inFlight: 0,
  };

  const initialQueueStats = {
    inbox: fill,
    'inbox-dlq': fill,
    sortByGeography: fill,
    'sortByGeography-dlq': fill,
    createProducts: fill,
    'createProducts-dlq': fill,
  };

  const initialTaskStats = [];

  useEffect(() => {
    // initial calls
    checkQueues();
    checkTasks();
  }, []);

  const [queueStats, updateQueueStats] = useState(initialQueueStats);
  const [taskStats, updateTaskStats] = useState(initialTaskStats);

  const checkQueues = () => {
    fetch(`http://localhost:4000/getQueueStats`)
      .then(response => response.json())
      .then(response => {
        updateQueueStats(response);
      })
      .catch(err => {
        console.log(err);
        alert('Problem querying sqs for queue stats.');
      });
  };

  const checkTasks = () => {
    fetch(`http://localhost:4000/getTaskInfo`)
      .then(response => response.json())
      .then(response => {
        updateTaskStats(response);
      })
      .catch(err => {
        console.log(err);
        alert('Problem querying ecs/fargate for task stats.');
      });
  };

  return (
    <Grid columns={2} divided>
      <Grid.Row>
        <Grid.Column>
          <h3 style={{ margin: 'auto' }}>Queue Stats</h3>
          <Table>
            <Table.Header>
              <Table.Row>
                <Table.HeaderCell>Queue Name</Table.HeaderCell>
                <Table.HeaderCell>Available</Table.HeaderCell>
                <Table.HeaderCell>In Flight</Table.HeaderCell>
                <Table.HeaderCell>DLQ</Table.HeaderCell>
              </Table.Row>
            </Table.Header>
            <Table.Body>
              <Table.Row>
                <Table.Cell>Inbox</Table.Cell>
                <Table.Cell>{queueStats.inbox.available}</Table.Cell>
                <Table.Cell>{queueStats.inbox.inFlight}</Table.Cell>
                <Table.Cell>{queueStats['inbox-dlq'].available}</Table.Cell>
              </Table.Row>
              <Table.Row>
                <Table.Cell>Sort</Table.Cell>
                <Table.Cell>{queueStats.sortByGeography.available}</Table.Cell>
                <Table.Cell>{queueStats.sortByGeography.inFlight}</Table.Cell>
                <Table.Cell>{queueStats['sortByGeography-dlq'].available}</Table.Cell>
              </Table.Row>
              <Table.Row>
                <Table.Cell>Product</Table.Cell>
                <Table.Cell>{queueStats.createProducts.available}</Table.Cell>
                <Table.Cell>{queueStats.createProducts.inFlight}</Table.Cell>
                <Table.Cell>{queueStats['createProducts-dlq'].available}</Table.Cell>
              </Table.Row>
            </Table.Body>
          </Table>
          <Button style={{ margin: '20px auto' }} onClick={checkQueues}>
            Refresh Queues
          </Button>
        </Grid.Column>
        <Grid.Column>
          <h3 style={{ margin: 'auto' }}>Container Stats</h3>
          {env !== 'test' ? (
            <div style={{ marginTop: '14px' }}>
              {!taskStats.length ? (
                <p>No tasks found.</p>
              ) : (
                <Table>
                  <Table.Header>
                    <Table.Row>
                      <Table.HeaderCell width="5">ID</Table.HeaderCell>
                      <Table.HeaderCell width="4">Desired Status</Table.HeaderCell>
                      <Table.HeaderCell width="4">Last Status</Table.HeaderCell>
                      <Table.HeaderCell width="3">Meta</Table.HeaderCell>
                    </Table.Row>
                  </Table.Header>
                  <Table.Body>
                    {taskStats.map(task => {
                      return (
                        <Table.Row key={task.id}>
                          <Table.Cell>{task.id}</Table.Cell>
                          <Table.Cell>{task.desiredStatus}</Table.Cell>
                          <Table.Cell>{task.lastStatus}</Table.Cell>
                          <Table.Cell>
                            <Popup
                              content={
                                <Table>
                                  <Table.Body>
                                    <Table.Row>
                                      <Table.Cell>Created:</Table.Cell>
                                      <Table.Cell>
                                        {new Date(task.createdAt).toLocaleTimeString()}
                                      </Table.Cell>
                                    </Table.Row>
                                    <Table.Row>
                                      <Table.Cell>Pull Started:</Table.Cell>
                                      <Table.Cell>
                                        {new Date(task.pullStartedAt).toLocaleTimeString()}
                                      </Table.Cell>
                                    </Table.Row>
                                    <Table.Row>
                                      <Table.Cell>Pull Stopped:</Table.Cell>
                                      <Table.Cell>
                                        {new Date(task.pullStoppedAt).toLocaleTimeString()}
                                      </Table.Cell>
                                    </Table.Row>
                                    <Table.Row>
                                      <Table.Cell>Task Started:</Table.Cell>
                                      <Table.Cell>
                                        {new Date(task.startedAt).toLocaleTimeString()}
                                      </Table.Cell>
                                    </Table.Row>
                                  </Table.Body>
                                </Table>
                              }
                              trigger={<Icon name="hourglass outline" />}
                            />
                            <Popup
                              content={
                                <Table>
                                  <Table.Body>
                                    <Table.Row>
                                      <Table.Cell>CPU:</Table.Cell>
                                      <Table.Cell>{task.cpu}</Table.Cell>
                                    </Table.Row>
                                    <Table.Row>
                                      <Table.Cell>Memory:</Table.Cell>
                                      <Table.Cell>{task.memory}</Table.Cell>
                                    </Table.Row>
                                  </Table.Body>
                                </Table>
                              }
                              trigger={<Icon name="microchip" />}
                            />
                            <Popup
                              content={
                                <Table>
                                  <Table.Body>
                                    <Table.Row>
                                      <Table.Cell>Launch Type</Table.Cell>
                                      <Table.Cell>{task.launchType}</Table.Cell>
                                    </Table.Row>
                                    <Table.Row>
                                      <Table.Cell>Task Arn</Table.Cell>
                                      <Table.Cell>{task.taskArn}</Table.Cell>
                                    </Table.Row>
                                    <Table.Row>
                                      <Table.Cell>Definition</Table.Cell>
                                      <Table.Cell>{task.taskDefinitionArn}</Table.Cell>
                                    </Table.Row>
                                  </Table.Body>
                                </Table>
                              }
                              trigger={<Icon name="tags" />}
                            />
                          </Table.Cell>
                        </Table.Row>
                      );
                    })}
                  </Table.Body>
                </Table>
              )}
              <Button style={{ margin: '20px' }} onClick={checkTasks}>
                Refresh Containers
              </Button>
              <Button style={{ margin: '20px' }}>Launch New Container</Button>
            </div>
          ) : (
            <p>Not available for test environment. </p>
          )}
        </Grid.Column>
      </Grid.Row>
    </Grid>
  );
}

export default Launch;
