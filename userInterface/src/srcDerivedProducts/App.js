import React, { useState } from 'react';
import { Input, Label } from 'semantic-ui-react';
import StatsLayout from './StatsLayout';
import DownloadsTable from './DownloadsTable';
import Mapper from './Mapper';

function App({ env }) {
  const [inputVal, updateInputVal] = useState('');
  const [statFiles, updateStatFiles] = useState([]);
  const [selectedDownload, updateSelectedDownload] = useState(null);
  const [statsInfo, updateStatsInfo] = useState({});
  const [selectedFieldKey, updateSelectedFieldKey] = useState(null);
  const [attributeChosen, updateAttributeChosen] = useState(false);

  const handleClick = (evt, data) => {
    window
      .fetch('http://localhost:4000/queryStatFiles?geoid=' + inputVal)
      .then(res => res.json())
      .then(data => {
        updateStatFiles(data.rows);
      })
      .catch(err => {
        console.error('err:', err);
      });
  };

  return (
    <div>
      {attributeChosen === false ? (
        <div>
          <Label pointing="right">
            Enter FIPS or leave blank for all downloads missing derived products
          </Label>
          <Input
            style={{ padding: '10px' }}
            action={{
              icon: 'play',
              onClick: handleClick,
            }}
            placeholder="Search..."
            value={inputVal}
            onChange={(evt, data) => {
              updateInputVal(data.value);
            }}
          />
          <DownloadsTable
            statFiles={statFiles}
            selectedDownload={selectedDownload}
            updateSelectedDownload={updateSelectedDownload}
          />
          <br />
          {selectedDownload ? (
            <StatsLayout
              selectedDownload={selectedDownload}
              statsInfo={statsInfo}
              updateStatsInfo={updateStatsInfo}
              selectedFieldKey={selectedFieldKey}
              updateSelectedFieldKey={updateSelectedFieldKey}
              updateAttributeChosen={updateAttributeChosen}
              env={env}
            />
          ) : (
            <span />
          )}
        </div>
      ) : (
        <Mapper
          statsInfo={statsInfo}
          selectedFieldKey={selectedFieldKey}
          geoid={selectedDownload.geoid.slice(0, 2)}
          selectedDownload={selectedDownload}
        />
      )}
    </div>
  );
}

export default App;
