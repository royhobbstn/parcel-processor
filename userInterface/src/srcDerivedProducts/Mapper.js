// @ts-check

import React, { useState, useEffect } from 'react';
import { Card, Table, Grid, Button, Icon, Modal, Header } from 'semantic-ui-react';

function Mapper({ statsInfo, selectedFieldKey, geoid, selectedDownload }) {
  const modal = {
    missingAttributes: [],
    missingGeoids: [],
    countOfPossible: 0,
    countOfUniqueGeoids: 0,
    attributesUsingSameGeoid: [],
    mapping: {},
  };

  const [geographies, updateGeographies] = useState([]);
  const [rowMarker, updateRowMarker] = useState(0);
  const [fieldMap, updateFieldMap] = useState([]);
  const [disableCreate, updateDisableCreate] = useState(false);
  const [modalIsOpen, updateModalIsOpen] = useState(false);
  const [modalStatsObj, updateModalStatsObj] = useState(modal);

  const handleClick = (evt, data, geoid) => {
    fieldMap[rowMarker] = geoid;
    const numItems = Object.keys(statsInfo.fields[selectedFieldKey].uniques).length;
    if (rowMarker === numItems - 1) {
      updateRowMarker(0);
    } else {
      updateRowMarker(rowMarker + 1);
    }
  };

  const sendMapping = (evt, data) => {
    updateDisableCreate(true);
    updateModalIsOpen(false);

    // create payload
    const payload = {
      selectedFieldKey,
      selectedDownload,
      modalStatsObj,
      geographies,
    };

    fetch(`http://localhost:4000/sendSortSQS`, {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      method: 'POST',
      body: JSON.stringify(payload),
    })
      .then(response => response.json())
      .then(() => {
        alert('Successfully sent product data to SQS.');
      })
      .catch(err => {
        console.log(err);
        alert('Problem sending product data to SQS.');
      });
  };

  const updateModalStats = () => {
    const keys = Object.keys(statsInfo.fields[selectedFieldKey].uniques).sort();

    const mapping = {};
    const missingAttributes = [];
    const missingGeoids = [];
    const uniqueGeoidsUsed = new Set();
    const geoidsMap = {};
    keys.forEach((key, index) => {
      if (fieldMap[index]) {
        mapping[key] = fieldMap[index];
        uniqueGeoidsUsed.add(fieldMap[index]);
        if (geoidsMap[fieldMap[index]]) {
          geoidsMap[fieldMap[index]].push(key);
        } else {
          geoidsMap[fieldMap[index]] = [key];
        }
      } else {
        missingAttributes.push(key);
      }
    });

    const countOfPossible = geographies.length;
    const countOfUniqueGeoids = uniqueGeoidsUsed.size;

    geographies.forEach(geo => {
      if (!uniqueGeoidsUsed.has(geo.geoid)) {
        missingGeoids.push(geo.geoid);
      }
    });

    const attributesUsingSameGeoid = [];
    Object.keys(geoidsMap).forEach(key => {
      if (geoidsMap[key].length > 1) {
        attributesUsingSameGeoid.push({ geoid: key, attributes: geoidsMap[key] });
      }
    });

    updateModalStatsObj({
      missingAttributes,
      missingGeoids,
      countOfPossible,
      countOfUniqueGeoids,
      attributesUsingSameGeoid,
      mapping,
    });
  };

  useEffect(() => {
    // load possible sub-geographies from a given parent geography
    fetch(`http://localhost:4000/getSubGeographies?geoid=${geoid}`)
      .then(res => res.json())
      .then(res => {
        updateGeographies(res);
      })
      .catch(err => {
        console.error(err);
      });
  }, [geoid]);

  return (
    <div>
      <Grid columns={2} divided>
        <Grid.Row>
          <Grid.Column width={9}>
            <Card>
              <Card.Content>
                <Card.Header>{selectedFieldKey}</Card.Header>
                <Card.Meta>{statsInfo.fields[selectedFieldKey].types.join(', ')}</Card.Meta>
              </Card.Content>
            </Card>
          </Grid.Column>
          <Grid.Column width={7}>
            <Button
              className="ui right floated"
              size="small"
              color="blue"
              disabled={disableCreate}
              onClick={() => {
                updateModalStats();
                updateModalIsOpen(true);
              }}
            >
              Create
            </Button>
          </Grid.Column>
        </Grid.Row>
        <Grid.Row>
          <Grid.Column width={9}>
            <div style={{ height: '420px', overflowY: 'scroll', border: '1px dotted grey' }}>
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
                              const newArr = [...fieldMap];
                              newArr[index] = '';
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
            <div style={{ height: '420px', overflowY: 'scroll', border: '1px dotted grey' }}>
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
      <Modal
        open={modalIsOpen}
        closeOnEscape={true}
        closeOnDimmerClick={true}
        onClose={() => {
          updateModalIsOpen(false);
        }}
      >
        <Modal.Content>
          <Modal.Description>
            <Header>Product Creation Summary</Header>
            <p>
              Creating {modalStatsObj.countOfUniqueGeoids} unique subgeographies out of{' '}
              {modalStatsObj.countOfPossible} possible
            </p>
            {modalStatsObj.missingAttributes.length > 0 ? (
              <div>
                <p>{modalStatsObj.missingAttributes.length} attribute(s) were ignored:</p>
                <ul>
                  {modalStatsObj.missingAttributes.map(d => {
                    return <li key={d}>{d}</li>;
                  })}
                </ul>
              </div>
            ) : null}
            {modalStatsObj.missingGeoids.length > 0 ? (
              <div>
                <p>{modalStatsObj.missingGeoids.length} geoid(s) were ignored:</p>
                <ul>
                  {modalStatsObj.missingGeoids.map(d => {
                    return <li key={d}>{d}</li>;
                  })}
                </ul>
              </div>
            ) : null}

            {modalStatsObj.attributesUsingSameGeoid.length > 0 ? (
              <div>
                <p>The same geoid was used on these multiple values:</p>
                <ul>
                  {modalStatsObj.attributesUsingSameGeoid.map(d => {
                    return (
                      <li key={d.geoid}>
                        <b>{d.geoid}</b>, {d.attributes.join(', ')}
                      </li>
                    );
                  })}
                </ul>
              </div>
            ) : null}
            <div style={{ margin: 'auto', width: '220px', height: '40px' }}>
              <Button
                style={{ width: '90px' }}
                floated="left"
                size="small"
                color="blue"
                onClick={sendMapping}
              >
                OK
              </Button>
              <Button
                style={{ width: '90px' }}
                floated="right"
                onClick={() => {
                  updateModalIsOpen(false);
                }}
              >
                Cancel
              </Button>
            </div>
          </Modal.Description>
        </Modal.Content>
      </Modal>
    </div>
  );
}

export default Mapper;
