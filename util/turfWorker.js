// @ts-check

const turf = require('@turf/turf');

process.on('message', messageObject => {
  // example
  if (messageObject.parentMsg === 'add') {
    try {
      const response = messageObject.params[0] + messageObject.params[1];
      process.send({ response, isError: false });
    } catch (err) {
      process.send({ response: err.message, isError: true });
    }
  }

  // turf functions
  if (messageObject.parentMsg === 'intersect') {
    try {
      const response = turf.intersect(messageObject.params[0], messageObject.params[1]);
      process.send({ response, isError: false });
    } catch (err) {
      process.send({ response: err.message, isError: true });
    }
  }

  if (messageObject.parentMsg === 'union') {
    try {
      const response = turf.union(messageObject.params[0], messageObject.params[1]);
      process.send({ response, isError: false });
    } catch (err) {
      process.send({ response: err.message, isError: true });
    }
  }

  if (messageObject.parentMsg === 'area') {
    try {
      const response = turf.area(messageObject.params[0]);
      process.send({ response, isError: false });
    } catch (err) {
      process.send({ response: err.message, isError: true });
    }
  }

  if (messageObject.parentMsg === 'flatten') {
    try {
      const response = turf.flatten(messageObject.params[0]);
      process.send({ response, isError: false });
    } catch (err) {
      process.send({ response: err.message, isError: true });
    }
  }

  if (messageObject.parentMsg === 'coordAll') {
    try {
      const response = turf.coordAll(messageObject.params[0]);
      process.send({ response, isError: false });
    } catch (err) {
      process.send({ response: err.message, isError: true });
    }
  }

  // end

  if (messageObject.parentMsg === 'end') {
    process.exit();
  }
});
