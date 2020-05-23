import React, { useState, useEffect } from 'react';

import { subscribeToTimer } from './test';

const App = () => {

	const [timestamp, setTimestamp] = useState(0);

	useEffect(()=> {
		subscribeToTimer((err, updatedTimestamp) => {
			if(err) {
			  console.log(err);
			}
			setTimestamp(updatedTimestamp);
		  });
	}, []);

	return (
		<div>
		  <p>
		  This is the timer value: {timestamp}
		  </p>
		</div>
	  );
}

export default App;