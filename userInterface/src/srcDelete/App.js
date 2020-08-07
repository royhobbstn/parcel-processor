// @ts-check

import React, { useState } from 'react';

import { Button, Input, Table } from 'semantic-ui-react';

function Delete({ env }) {
  const [inputVal, updateInputVal] = useState('');

  const [busy, updateBusy] = useState(false);
  const [taskRecords, updateTaskRecords] = useState([]);

  function byDownloadRef() {
    if (!inputVal) {
      return;
    }
    //
    updateBusy(true);

    // query sourceChecks
    // query downloads
    // query products
  }

  function byProductRef() {
    if (!inputVal) {
      return;
    }
    updateBusy(true);
    //
    fetch(`http://localhost:4000/byProductRef?ref=${inputVal}`)
      .then(response => response.json())
      .then(res => {
        console.log(res);
        updateTaskRecords([...mapToDbDeletes(res, false), ...mapToS3Deletes(res, false)]);
      })
      .catch(err => {
        console.log(err);
        alert('Problem query product individual records.');
      })
      .finally(() => {
        updateBusy(false);
      });
  }

  function byIndividualRef() {
    if (!inputVal) {
      return;
    }
    updateBusy(true);
    //
    fetch(`http://localhost:4000/byProductIndividualRef?ref=${inputVal}`)
      .then(response => response.json())
      .then(res => {
        console.log(res);
        updateTaskRecords([...mapToDbDeletes(res, true), ...mapToS3Deletes(res, true)]);
      })
      .catch(err => {
        console.log(err);
        alert('Problem query product individual records.');
      })
      .finally(() => {
        updateBusy(false);
      });
  }

  function mapToDbDeletes(arr, filterOutNdGeoJson) {
    const deletes = [];

    arr
      .filter(d => {
        if (filterOutNdGeoJson) {
          return d.product_type !== 'ndgeojson';
        }
        return true;
      })
      .forEach(item => {
        deletes.push({
          task_name: 'product_row',
          table_name: 'products',
          record_id: item.product_id,
          product_type: item.product_type,
          bucket_name: '',
          bucket_key: '',
          geoid: item.geoid,
          env,
        });
        deletes.push({
          task_name: 'logfile_row',
          table_name: 'logfiles',
          record_id: item.product_id,
          meta: 'join on products_id',
          product_type: item.product_type,
          bucket_name: '',
          bucket_key: '',
          geoid: item.geoid,
          env,
        });
      });

    return deletes;
  }

  function mapToS3Deletes(arr, filterOutNdGeoJson) {
    return arr
      .filter(d => {
        if (filterOutNdGeoJson) {
          return d.product_type !== 'ndgeojson';
        }
        return true;
      })
      .map(item => {
        return {
          task_name: item.product_type === 'pbf' ? 'tile_directory' : 'product_file',
          table_name: '',
          record_id: '',
          product_type: item.product_type,
          bucket_name: 'data-products-po',
          bucket_key: item.product_key,
          geoid: item.geoid,
          env,
        };
      });
  }

  function confirmDelete() {
    //
  }

  function reset() {
    updateBusy(false);
    updateTaskRecords([]);
    updateInputVal('');
  }

  return (
    <div style={{ margin: 'auto' }}>
      <br />
      <Input
        style={{ padding: '10px' }}
        placeholder="Reference Number..."
        value={inputVal}
        onClick={() => {
          reset();
        }}
        onChange={(evt, data) => {
          updateInputVal(data.value);
        }}
      />
      <br />
      <Button
        style={{ width: '200px', display: 'inline', margin: '20px' }}
        onClick={() => {
          byDownloadRef();
        }}
      >
        By Download Ref
      </Button>
      <Button
        style={{ width: '200px', display: 'inline', margin: '20px' }}
        onClick={() => {
          byProductRef();
        }}
      >
        By Product Ref
      </Button>
      <Button
        style={{ width: '200px', display: 'inline', margin: '20px' }}
        onClick={() => {
          byIndividualRef();
        }}
      >
        By Individual Ref
      </Button>
      {!busy ? (
        <div style={{ width: '100%' }}>
          {taskRecords.length ? (
            <div style={{ width: '100%' }}>
              <div
                style={{
                  height: '280px',
                  border: '1px dotted grey',
                  overflowY: 'scroll',
                  marginTop: '10px',
                }}
              >
                <Table>
                  <Table.Header>
                    <Table.Row>
                      <Table.HeaderCell>Task Name</Table.HeaderCell>
                      <Table.HeaderCell>Table</Table.HeaderCell>
                      <Table.HeaderCell>Product Type</Table.HeaderCell>
                      <Table.HeaderCell>Bucket</Table.HeaderCell>
                      <Table.HeaderCell>S3 Key</Table.HeaderCell>
                      <Table.HeaderCell>Geoid</Table.HeaderCell>
                    </Table.Row>
                  </Table.Header>
                  <Table.Body>
                    {taskRecords.map((result, index) => {
                      return (
                        <Table.Row key={index}>
                          <Table.Cell>{result.task_name}</Table.Cell>
                          <Table.Cell>{result.table_name}</Table.Cell>
                          <Table.Cell>{result.product_type}</Table.Cell>
                          <Table.Cell>{result.bucket_name}</Table.Cell>
                          <Table.Cell>{result.bucket_key}</Table.Cell>
                          <Table.Cell>{result.geoid}</Table.Cell>
                        </Table.Row>
                      );
                    })}
                  </Table.Body>
                </Table>
              </div>
              <br />
              <Button
                style={{ width: '200px', display: 'inline', margin: '20px' }}
                onClick={() => {
                  confirmDelete();
                }}
              >
                Confirm Delete
              </Button>
              <Button
                style={{ width: '200px', display: 'inline', margin: '20px' }}
                onClick={() => {
                  reset();
                }}
              >
                Reset
              </Button>
            </div>
          ) : (
            <div style={{ width: '100%', border: '1px dotted grey' }}>
              <br />
              <br />
              <br />
              <h3 style={{ margin: 'auto', width: '100px' }}>No Results</h3>
              <br />
              <br />
              <br />
            </div>
          )}
        </div>
      ) : (
        <div style={{ width: '100%', border: '1px dotted grey' }}>
          <br />
          <br />
          <br />
          <h3 style={{ margin: 'auto', width: '200px' }}>Loading Delete Plan</h3>
          <br />
          <br />
          <br />
        </div>
      )}
    </div>
  );
}

export default Delete;
