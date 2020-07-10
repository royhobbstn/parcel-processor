// @ts-check

import React from 'react';
import { Segment, Icon } from 'semantic-ui-react';

function EnvBar({ env, updateApp, online }) {
  const devStyle = { color: 'white', backgroundColor: 'green' };
  const prodStyle = { color: 'white', backgroundColor: 'red' };
  const testStyle = { color: 'white', backgroundColor: 'blue' };
  const downStyle = { color: 'white', backgroundColor: 'black' };
  const devText = 'You are running in Development mode.';
  const prodText = 'You are running in Production mode.';
  const testText = 'You are running in Local mode.';
  const downText = 'Server is down.';

  let dbIcon;

  if (online === 'yes') {
    dbIcon = <Icon style={{ float: 'right' }} name="thumbs up outline" size="large" />;
  } else if (online === 'no') {
    dbIcon = <Icon style={{ float: 'right' }} name="exclamation circle" size="large" />;
  } else {
    dbIcon = <Icon style={{ float: 'right' }} loading name="spinner" size="large" />;
  }

  let style;
  let text;

  if (env === 'development') {
    style = devStyle;
    text = devText;
  } else if (env === 'production') {
    style = prodStyle;
    text = prodText;
  } else if (env === 'test') {
    style = testStyle;
    text = testText;
  } else {
    style = downStyle;
    text = downText;
  }

  return (
    <Segment style={style}>
      <Icon
        name="home"
        link
        size="large"
        style={{ marginRight: '40px' }}
        onClick={() => {
          updateApp('');
        }}
      />
      {text}
      {dbIcon}
    </Segment>
  );
}

export default EnvBar;
