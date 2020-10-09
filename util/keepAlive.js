// @ts-check

const { initiateVisibilityHeartbeat } = require('./sqsOperations');
const { initiateDatabaseHeartbeat } = require('./wrapQuery');

let interval;
let databaseInterval;

process.on('message', messageObject => {
  if (messageObject.msg === 'start') {
    console.log(messageObject);
    // start
    const ctx = { log: console, process: [], timeBank: {}, timeStack: [] };
    interval = initiateVisibilityHeartbeat(ctx, messageObject.data, 60000, 150);
    databaseInterval = initiateDatabaseHeartbeat(ctx, 180);
  }

  if (messageObject.msg === 'end') {
    // discontinue updating visibility timeout
    clearInterval(interval);
    clearInterval(databaseInterval);
    process.exit();
  }
});
