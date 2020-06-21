import React from 'react';
import { Segment } from 'semantic-ui-react';

function EnvBar({ env }) {
  const devStyle = { color: 'white', backgroundColor: 'green' };
  const prodStyle = { color: 'white', backgroundColor: 'red' };
  const downStyle = { color: 'white', backgroundColor: 'black' };
  const devText = 'You are running in Development mode.';
  const prodText = 'You are running in Production mode.';
  const downText = 'Server is down.';

  let style;
  let text;

  if (env === 'development') {
    style = devStyle;
    text = devText;
  } else if (env === 'production') {
    style = prodStyle;
    text = prodText;
  } else {
    style = downStyle;
    text = downText;
  }

  return <Segment style={style}>{text}</Segment>;
}

export default EnvBar;
