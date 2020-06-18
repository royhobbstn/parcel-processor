import React from 'react';
import { Card, Table, Grid, Button } from 'semantic-ui-react';

function Mapper({ statsInfo, selectedFieldKey }) {
  // todo load counties per state

  return (
    <div>
      <Card>
        <Card.Content>
          <Card.Header>{selectedFieldKey}</Card.Header>
          <Card.Meta>{statsInfo.fields[selectedFieldKey].types.join(', ')}</Card.Meta>
        </Card.Content>
      </Card>
      <Grid columns={2} divided>
        <Grid.Row>
          <Grid.Column>
            <div style={{ height: '450px', overflowY: 'scroll', border: '1px dotted grey' }}>
              <Table selectable celled size="small">
                <Table.Body>
                  {Object.keys(statsInfo.fields[selectedFieldKey].uniques)
                    .sort()
                    .map(uniquesKey => {
                      return (
                        <Table.Row key={uniquesKey}>
                          <Table.Cell>{uniquesKey}</Table.Cell>
                          <Table.Cell textAlign="right">
                            {statsInfo.fields[selectedFieldKey].uniques[
                              uniquesKey
                            ].toLocaleString()}
                          </Table.Cell>
                          <Table.Cell textAlign="right">
                            {(
                              (statsInfo.fields[selectedFieldKey].uniques[uniquesKey] /
                                statsInfo.rowCount) *
                              100
                            ).toFixed(2)}
                            &nbsp;%
                          </Table.Cell>
                        </Table.Row>
                      );
                    })}
                </Table.Body>
              </Table>
            </div>
          </Grid.Column>
          <Grid.Column>
            <div style={{ height: '450px', overflowY: 'scroll', border: '1px dotted grey' }}>
              <Table selectable celled size="small">
                <Table.Body>
                  {Object.keys(statsInfo.fields[selectedFieldKey].uniques)
                    .sort()
                    .map(uniquesKey => {
                      return (
                        <Table.Row key={uniquesKey}>
                          <Table.Cell>
                            <Button circular size="mini" icon="plus" />
                          </Table.Cell>
                          <Table.Cell textAlign="right">1</Table.Cell>
                          <Table.Cell>2</Table.Cell>
                        </Table.Row>
                      );
                    })}
                </Table.Body>
              </Table>
            </div>
          </Grid.Column>
        </Grid.Row>
      </Grid>
    </div>
  );
}

export default Mapper;
