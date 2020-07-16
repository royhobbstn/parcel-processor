// @ts-check

import React, { useState, useEffect } from 'react';
import { Grid, Table, Button } from 'semantic-ui-react';

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

  const initialContainerStats = { running: 0, pending: 0 };

  useEffect(() => {
    // initial calls
    checkQueues();
    // checkContainers();
  }, []);

  const [queueStats, updateQueueStats] = useState(initialQueueStats);
  const [containerStats, updateContainerStats] = useState(initialContainerStats);

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
            <div>
              <Table>
                <Table.Header>
                  <Table.Row>
                    <Table.HeaderCell>Running Tasks</Table.HeaderCell>
                    <Table.HeaderCell>Pending Tasks</Table.HeaderCell>
                  </Table.Row>
                </Table.Header>
                <Table.Body>
                  <Table.Row>
                    <Table.Cell>{containerStats.running}</Table.Cell>
                    <Table.Cell>{containerStats.pending}</Table.Cell>
                  </Table.Row>
                </Table.Body>
              </Table>
              <Button style={{ margin: '20px' }}>Refresh Containers</Button>
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
