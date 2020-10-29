// @ts-check

const { initiateVisibilityHeartbeat } = require('./sqsOperations');
const { initiateDatabaseHeartbeat } = require('./wrapQuery');
const { initiateFreeMemoryQuery, initiateDiskSpaceQuery } = require('./misc');

let interval;
let databaseInterval;
let freeMemoryQuery;
let diskSpaceQuery;

process.on('message', messageObject => {
  if (messageObject.msg === 'start') {
    console.log(messageObject);
    // start
    const ctx = { log: console, process: [], timeBank: {}, timeStack: [] };
    interval = initiateVisibilityHeartbeat(ctx, messageObject.data, 60000, 150);
    databaseInterval = initiateDatabaseHeartbeat(ctx, 180);
    freeMemoryQuery = initiateFreeMemoryQuery(ctx, 60);
    diskSpaceQuery = initiateDiskSpaceQuery(ctx, 45);
  }

  if (messageObject.msg === 'end') {
    // discontinue updating visibility timeout
    clearInterval(interval);
    clearInterval(databaseInterval);
    clearInterval(freeMemoryQuery);
    clearInterval(diskSpaceQuery);
    process.exit();
  }
});
