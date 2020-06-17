import React, { useState } from 'react';
import { Input, Label } from 'semantic-ui-react';
import StatsLayout from './StatsLayout';
import DownloadsTable from './DownloadsTable';

function App() {
  const [inputVal, updateInputVal] = useState('');
  const [statFiles, updateStatFiles] = useState([]);
  const [selectedDownload, updateSelectedDownload] = useState(null);

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
    <div style={{ padding: '20px' }}>
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
      {selectedDownload ? <StatsLayout selectedDownload={selectedDownload} /> : <span />}
    </div>
  );
}

export default App;
