// @ts-check

const AWS = require('aws-sdk');
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

// TODO

// runTask

// startTask

const forReference = {
  tasks: [
    {
      attachments: [
        {
          id: '36643d5f-1941-4da5-9d44-8f979fac1bd4',
          type: 'ElasticNetworkInterface',
          status: 'ATTACHED',
          details: [
            {
              name: 'subnetId',
              value: 'subnet-2334cf48',
            },
            {
              name: 'networkInterfaceId',
              value: 'eni-05a471f4c22b7426a',
            },
            {
              name: 'macAddress',
              value: '02:4c:64:04:a6:b0',
            },
            {
              name: 'privateIPv4Address',
              value: '172.31.13.176',
            },
          ],
        },
      ],
      availabilityZone: 'us-east-2a',
      capacityProviderName: 'FARGATE',
      clusterArn: 'arn:aws:ecs:us-east-2:000009394762:cluster/parcel-processor',
      connectivity: 'CONNECTED',
      connectivityAt: '2020-07-16T22:49:27.711Z',
      containers: [
        {
          containerArn:
            'arn:aws:ecs:us-east-2:000009394762:container/2ef1a131-b53f-4a04-9c32-d0675d3bd0ac',
          taskArn: 'arn:aws:ecs:us-east-2:000009394762:task/ecf1754d-b239-4f35-8a48-a5b73304660b',
          name: 'processor-dev',
          image: '000009394762.dkr.ecr.us-east-2.amazonaws.com/parcel-outlet:latest',
          imageDigest: 'sha256:8e115042a914d436690db171d3fc5bdfd5785ac43ba02b5cd851533399655cb8',
          runtimeId: 'ecf1754d-b239-4f35-8a48-a5b73304660b-1500111173',
          lastStatus: 'RUNNING',
          networkBindings: [],
          networkInterfaces: [
            {
              attachmentId: '36643d5f-1941-4da5-9d44-8f979fac1bd4',
              privateIpv4Address: '172.31.13.176',
            },
          ],
          healthStatus: 'UNKNOWN',
          cpu: '2048',
          memory: '8000',
          memoryReservation: '6000',
        },
      ],
      cpu: '2048',
      createdAt: '2020-07-16T22:49:24.171Z',
      desiredStatus: 'RUNNING',
      group: 'family:processor-dev',
      healthStatus: 'UNKNOWN',
      lastStatus: 'RUNNING',
      launchType: 'FARGATE',
      memory: '8192',
      overrides: {
        containerOverrides: [
          {
            name: 'processor-dev',
          },
        ],
        inferenceAcceleratorOverrides: [],
      },
      platformVersion: '1.4.0',
      pullStartedAt: '2020-07-16T22:49:42.853Z',
      pullStoppedAt: '2020-07-16T22:51:01.853Z',
      startedAt: '2020-07-16T22:51:11.853Z',
      tags: [],
      taskArn: 'arn:aws:ecs:us-east-2:000009394762:task/ecf1754d-b239-4f35-8a48-a5b73304660b',
      taskDefinitionArn: 'arn:aws:ecs:us-east-2:000009394762:task-definition/processor-dev:16',
      version: 4,
    },
  ],
  failures: [],
};
