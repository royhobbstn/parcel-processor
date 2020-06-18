import React, { useState, useEffect } from 'react';
import { Card, Table, Grid, Button, Icon } from 'semantic-ui-react';

function Mapper({ statsInfo, selectedFieldKey, geoid }) {
  const [geographies, updateGeographies] = useState([]);
  const [rowMarker, updateRowMarker] = useState(0);
  const [fieldMap, updateFieldMap] = useState([]);

  const handleClick = (evt, data, geoid) => {
    console.log(geoid);
    fieldMap[rowMarker] = geoid;
    updateRowMarker(rowMarker + 1);
  };

  useEffect(() => {
    console.log({ geoid });
    // load possible sub-geographies from a given parent geography
    fetch(`http://localhost:4000/getSubGeographies?geoid=${geoid}`)
      .then(res => res.json())
      .then(res => {
        console.log(res);
        updateGeographies(res);
      })
      .catch(err => {
        console.error(err);
      });
  }, [geoid]);

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
          <Grid.Column width={9}>
            <div style={{ height: '450px', overflowY: 'scroll', border: '1px dotted grey' }}>
              <Table selectable celled size="small">
                <Table.Body>
                  {Object.keys(statsInfo.fields[selectedFieldKey].uniques)
                    .sort()
                    .map((uniquesKey, index) => {
                      return (
                        <Table.Row key={uniquesKey}>
                          <Table.Cell
                            width="1"
                            onClick={() => {
                              updateRowMarker(index);
                            }}
                          >
                            {rowMarker === index ? <Icon name="caret right" /> : null}
                          </Table.Cell>
                          <Table.Cell width="6">{uniquesKey}</Table.Cell>
                          <Table.Cell width="3" textAlign="right">
                            {statsInfo.fields[selectedFieldKey].uniques[
                              uniquesKey
                            ].toLocaleString()}
                          </Table.Cell>
                          <Table.Cell width="3" textAlign="right">
                            {(
                              (statsInfo.fields[selectedFieldKey].uniques[uniquesKey] /
                                statsInfo.rowCount) *
                              100
                            ).toFixed(2)}
                            &nbsp;%
                          </Table.Cell>
                          <Table.Cell
                            onDoubleClick={() => {
                              console.log({ index });
                              const newArr = [...fieldMap];
                              newArr[index] = '';
                              console.log({ newArr });
                              updateFieldMap(newArr);
                            }}
                            width="3"
                          >
                            {fieldMap[index]}
                          </Table.Cell>
                        </Table.Row>
                      );
                    })}
                </Table.Body>
              </Table>
            </div>
          </Grid.Column>
          <Grid.Column width={7}>
            <div style={{ height: '450px', overflowY: 'scroll', border: '1px dotted grey' }}>
              <Table selectable celled size="small">
                <Table.Body>
                  {geographies.map(geog => {
                    return (
                      <Table.Row key={geog.geoid}>
                        <Table.Cell width="1">
                          <Button
                            circular
                            size="mini"
                            icon="plus"
                            onClick={(evt, data) => {
                              handleClick(evt, data, geog.geoid);
                            }}
                          />
                        </Table.Cell>
                        <Table.Cell width="5" textAlign="center">
                          {geog.geoid}
                        </Table.Cell>
                        <Table.Cell width="10">{geog.geoname}</Table.Cell>
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
