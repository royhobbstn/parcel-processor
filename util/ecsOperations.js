// @ts-check

const AWS = require('aws-sdk');
const config = require('config');
const ecs = new AWS.ECS({ apiVersion: '2014-11-13', region: 'us-east-2' });
const { unwindStack } = require('./misc');

exports.getTaskInfo = getTaskInfo;

function getTaskInfo(ctx) {
  ctx.process.push('getTaskInfo');

  return new Promise((resolve, reject) => {
    let family = 'processor-dev';
    if (process.env.NODE_ENV === 'production') {
      family = 'processor-prod';
    }

    var params = {
      cluster: 'parcel-processor',
      family,
    };
    ecs.listTasks(params, async function (err, data) {
      if (err) {
        return reject(err);
      }

      if (data.taskArns.length) {
        const results = await describeTasks(ctx, data.taskArns);

        const taskArray = results.tasks.map(task => {
          return {
            cpu: task.cpu,
            createdAt: task.createdAt,
            desiredStatus: task.desiredStatus,
            lastStatus: task.lastStatus,
            launchType: task.launchType,
            memory: task.memory,
            pullStartedAt: task.pullStartedAt,
            pullStoppedAt: task.pullStoppedAt,
            startedAt: task.startedAt,
            taskArn: task.taskArn,
            id: task.taskArn.split('/').slice(-1)[0],
            taskDefinitionArn: task.taskDefinitionArn,
          };
        });
        unwindStack(ctx.process, 'getTaskInfo');
        return resolve(taskArray);
      } else {
        unwindStack(ctx.process, 'getTaskInfo');
        return resolve([]);
      }
    });
  });
}

exports.describeTasks = describeTasks;

function describeTasks(ctx, taskArns) {
  ctx.process.push('describeTasks');

  return new Promise((resolve, reject) => {
    const params = {
      cluster: 'parcel-processor',
      tasks: taskArns,
    };

    ecs.describeTasks(params, function (err, data) {
      if (err) {
        return reject(err);
      }
      unwindStack(ctx.process, 'describeTasks');
      resolve(data);
    });
  });
}

exports.runProcessorTask = runProcessorTask;

async function runProcessorTask(ctx) {
  ctx.process.push('runProcessorTask');

  const taskDefinition = config.get('ECS.taskDefinition');

  return new Promise((resolve, reject) => {
    const params = {
      taskDefinition,
      cluster: 'parcel-processor',
      count: 1,
      launchType: 'FARGATE',
      platformVersion: '1.4.0',
      networkConfiguration: {
        awsvpcConfiguration: {
          subnets: ['subnet-c0db438c', 'subnet-2334cf48', 'subnet-a1cafbdb'],
          assignPublicIp: 'ENABLED',
          securityGroups: ['sg-abaf0fd3'],
        },
      },
    };

    ecs.runTask(params, function (err, data) {
      if (err) {
        return reject(err);
      }
      unwindStack(ctx.process, 'runProcessorTask');
      return resolve(data);
    });
  });
}
