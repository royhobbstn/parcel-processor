const { fork } = require('child_process');

let turfWorker = fork('./util/turfWorker.js');

const TIMEOUT = '__timeout';

async function runTurf(ctx, funcName, paramsArr) {
  const realPromise = new Promise((resolve, reject) => {
    turfWorker.on('message', response => {
      resolve(response);
    });

    turfWorker.on('error', err => {
      reject(err);
    });

    turfWorker.send({ parentMsg: funcName, params: paramsArr });
  });

  const timeoutPromise = new Promise((resolve, reject) => {
    setTimeout(resolve, 5000, { response: TIMEOUT, isError: true });
  });

  let response;

  try {
    const result = await Promise.race([realPromise, timeoutPromise]);
    if (result.response === TIMEOUT) {
      ctx.log.warn('Forked process stalled.', { funcName, paramsArr });
      turfWorker.kill('SIGINT');
      ctx.log.info('old process killed.  recreating worker');
      turfWorker = fork('../util/runTurf.js');
      response = null;
    } else if (result.isError) {
      ctx.log.warn('Forked process errored in routine operation.');
      response = null;
    } else {
      response = result.response;
    }
  } catch (err) {
    ctx.log.error('There was an unexpected error.  Restarting Forked Process.');
    turfWorker.kill('SIGINT');
    ctx.log.info('old process killed.  recreating worker');
    turfWorker = fork('../util/runTurf.js');
    response = null;
  }

  return response;
}

exports.runTurf = runTurf;

// runTurf('add', [5, 6]).then(response => {
//   console.log(response);
//   turfWorker.send({ parentMsg: 'end' });
// });
