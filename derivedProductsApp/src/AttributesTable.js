import React from 'react';
import { Table } from 'semantic-ui-react';

function AttributesTable({ statsInfo, selectedFieldKey, updateSelectedFieldKey }) {
  return (
    <div
      style={{
        height: '200px',
        overflowY: 'scroll',
        border: '1px dotted grey',
      }}
    >
      <Table selectable celled size="small">
        <Table.Body>
          {Object.keys(statsInfo.fields || {}).map(fieldKey => {
            return (
              <Table.Row
                key={fieldKey}
                onClick={() => {
                  if (fieldKey === selectedFieldKey) {
                    updateSelectedFieldKey(null);
                  } else {
                    updateSelectedFieldKey(fieldKey);
                  }
                }}
              >
                <Table.Cell>{fieldKey}</Table.Cell>
              </Table.Row>
            );
          })}
        </Table.Body>
      </Table>
    </div>
  );
}

export default AttributesTable;
