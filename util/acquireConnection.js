const { checkHealth } = require('./wrappers/wrapQuery');

exports.acquireConnection = async function () {
  // ping the database to make sure its up / get it ready
  // after that, keep-alives from data-api-client should do the rest
  const seconds = 10;
  let connected = false;
  do {
    console.log('attempting to connect to database');
    connected = await checkHealth();
    if (!connected) {
      console.log(`attempt failed.  trying again in ${seconds} seconds...`);
      await setPause(seconds * 1000);
    }
  } while (!connected);

  console.log('connected');
};

function setPause(timer) {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve();
    }, timer);
  });
}
