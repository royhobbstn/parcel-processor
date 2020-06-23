import React from 'react';
import { Table, Card, Button } from 'semantic-ui-react';

function AttributesInfoCard({ statsInfo, selectedFieldKey, updateAttributeChosen }) {
  return (
    <Card
      style={{
        height: '200px',
        overflowY: 'scroll',
        width: '100%',
      }}
    >
      <Card.Content>
        <Button
          className="ui right floated"
          size="small"
          color="blue"
          onClick={() => {
            updateAttributeChosen(true);
          }}
        >
          Map Field
        </Button>
        <Card.Header>{selectedFieldKey}</Card.Header>{' '}
        <Card.Meta>{statsInfo.fields[selectedFieldKey].types.join(', ')}</Card.Meta>
        <Table selectable celled size="small">
          <Table.Body>
            {Object.keys(statsInfo.fields[selectedFieldKey].uniques)
              .sort((a, b) => {
                return (
                  statsInfo.fields[selectedFieldKey].uniques[b] -
                  statsInfo.fields[selectedFieldKey].uniques[a]
                );
              })
              .map(uniquesKey => {
                return (
                  <Table.Row key={uniquesKey}>
                    <Table.Cell>{uniquesKey}</Table.Cell>
                    <Table.Cell textAlign="right">
                      {statsInfo.fields[selectedFieldKey].uniques[uniquesKey].toLocaleString()}
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
      </Card.Content>
    </Card>
  );
}

export default AttributesInfoCard;
