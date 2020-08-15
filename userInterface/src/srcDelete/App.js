// @ts-check

import React, { useState } from 'react';

import { Button, Input, Table, Icon } from 'semantic-ui-react';

function Delete({ env }) {
  const [inputVal, updateInputVal] = useState('');

  const [busy, updateBusy] = useState(false);
  const [taskRecords, updateTaskRecords] = useState([]);

  const [deleteBusy, updateDeleteBusy] = useState(false);

  async function byDownloadRef() {
    if (!inputVal) {
      alert('No value specified for reference number!');
      return;
    }
    updateTaskRecords([]);
    updateBusy(true);

    const downloads = fetch(
      `http://localhost:4000/productsByDownloadRef?ref=${inputVal}`,
    ).then(response => response.json());

    const products = fetch(
      `http://localhost:4000/downloadsByDownloadRef?ref=${inputVal}`,
    ).then(response => response.json());

    await Promise.all([downloads, products])
      .then(res => {
        updateTaskRecords([
          ...mapToDbDeletes(res[0], false),
          ...mapToS3Deletes(res[0], false),
          ...mapDownloadsToDbDeletes(res[1]),
          ...mapDownloadsToS3Deletes(res[1]),
        ]);
      })
      .catch(err => {
        console.log(err);
        alert('Problem querying download ref records.');
      })
      .finally(() => {
        updateBusy(false);
      });
  }

  function byProductRef() {
    if (!inputVal) {
      alert('No value specified for reference number!');
      return;
    }
    updateTaskRecords([]);
    updateBusy(true);
    //
    fetch(`http://localhost:4000/byProductRef?ref=${inputVal}`)
      .then(response => response.json())
      .then(res => {
        updateTaskRecords([...mapToDbDeletes(res, false), ...mapToS3Deletes(res, false)]);
      })
      .catch(err => {
        console.log(err);
        alert('Problem querying product individual records.');
      })
      .finally(() => {
        updateBusy(false);
      });
  }

  function byIndividualRef() {
    if (!inputVal) {
      alert('No value specified for reference number!');
      return;
    }
    updateTaskRecords([]);
    updateBusy(true);
    //
    fetch(`http://localhost:4000/byProductIndividualRef?ref=${inputVal}`)
      .then(response => response.json())
      .then(res => {
        updateTaskRecords([...mapToDbDeletes(res, true), ...mapToS3Deletes(res, true)]);
      })
      .catch(err => {
        console.log(err);
        alert('Problem querying product individual records.');
      })
      .finally(() => {
        updateBusy(false);
      });
  }

  function mapDownloadsToS3Deletes(arr) {
    return arr.map(item => {
      return {
        task_name: 'raw_file',
        table_name: '',
        record_id: '',
        product_type: '',
        bucket_name: 'raw-data-po',
        bucket_key: item.raw_key,
        geoid: '',
        env,
        priority: 1,
      };
    });
  }

  function mapDownloadsToDbDeletes(arr) {
    const deletes = [];
    arr.forEach(item => {
      deletes.push({
        task_name: 'download_row',
        table_name: 'downloads',
        record_id: item.download_id,
        product_type: '',
        bucket_name: '',
        bucket_key: '',
        geoid: '',
        env,
        priority: 4,
      });
      deletes.push({
        task_name: 'source-check_row',
        table_name: 'source_checks',
        record_id: item.check_id,
        product_type: '',
        bucket_name: '',
        bucket_key: '',
        geoid: '',
        env,
        priority: 5,
      });
    });

    return deletes;
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
          priority: 3,
        });
      });

    return deletes;
  }

  function mapToS3Deletes(arr, filterOutNdGeoJson) {
    const s3deletes = [];

    arr
      .filter(d => {
        if (filterOutNdGeoJson) {
          return d.product_type !== 'ndgeojson';
        }
        return true;
      })
      .forEach(item => {
        s3deletes.push({
          task_name: item.product_type === 'pbf' ? 'tile_directory' : 'product_file',
          table_name: '',
          record_id: '',
          product_type: item.product_type,
          bucket_name: item.product_type === 'pbf' ? 'tile-server-po' : 'data-products-po',
          bucket_key: item.product_key,
          geoid: item.geoid,
          env,
          priority: 1,
        });

        if (item.product_type === 'ndgeojson') {
          // stat file
          s3deletes.push({
            task_name: 'stat_file',
            table_name: '',
            record_id: '',
            product_type: item.product_type,
            bucket_name: 'data-products-po',
            bucket_key: item.product_key,
            geoid: item.geoid,
            env,
            priority: 1,
          });
        }
      });

    return s3deletes;
  }

  function confirmDelete() {
    const countS3 = taskRecords.filter(d => d.bucket_name).length;

    const confirmed = window.confirm(
      `Are you sure you wnat to delete these ${
        taskRecords.length - countS3
      } database rows and ${countS3} S3 Objects?`,
    );

    if (confirmed) {
      updateDeleteBusy(true);

      const fetchUrl = `http://localhost:4000/deleteSelectedItems`;

      fetch(fetchUrl, {
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
        },
        method: 'POST',
        body: JSON.stringify(taskRecords),
      })
        .then(response => response.json())
        .then(res => {
          console.log('Deletion succeeded', res);
        })
        .catch(err => {
          console.log(err);
          alert('Problem deleting selected items.');
        })
        .finally(() => {
          updateDeleteBusy(false);
        });
    }
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
              {deleteBusy ? (
                <div style={{ width: '100%' }}>
                  <Icon style={{ margin: 'auto' }} loading name="spinner" size="large" />
                </div>
              ) : (
                <div>
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
              )}
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
