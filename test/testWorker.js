const { CostExplorer } = require('aws-sdk');
const { fork } = require('child_process');

let turfWorker = fork('../util/runTurf.js');

const TIMEOUT = '__timeout';

async function main() {
  const realPromise = new Promise((resolve, reject) => {
    turfWorker.on('message', response => {
      resolve(response);
    });

    turfWorker.on('error', err => {
      console.log('rejecting');
      reject(err);
    });

    turfWorker.send({ parentMsg: 'err', params: [5, 6] });
  });

  const timeoutPromise = new Promise((resolve, reject) => {
    setTimeout(resolve, 5000, { response: TIMEOUT, isError: true });
  });

  let response;

  try {
    response = await Promise.race([realPromise, timeoutPromise]);
    if (response.isError) {
      console.log('Forked process stalled.');
      turfWorker.kill('SIGINT');
      console.log('old process killed.  recreating worker');
      turfWorker = fork('../util/runTurf.js');
      response = null;
    }
  } catch (err) {
    console.log('There was an unexpected error.  Restarting Forked Process.');
    turfWorker.kill('SIGINT');
    console.log('old process killed.  recreating worker');
    turfWorker = fork('../util/runTurf.js');
    response = null;
  }
  console.log(response);
}

main();
