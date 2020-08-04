// @ts-check

export const sqsQueues = [
  {
    key: 'inbox',
    text: 'Inbox',
    value: 'sendInboxSQS',
  },
  {
    key: 'products',
    text: 'Products',
    value: 'sendProductSQS',
  },
  {
    key: 'sort',
    text: 'Sort',
    value: 'sendSortSQS',
  },
];

export const sqsDlqQueues = [
  {
    key: 'inbox',
    text: 'Inbox',
    value: 'viewInboxDlq',
  },
  {
    key: 'products',
    text: 'Products',
    value: 'viewProductDlq',
  },
  {
    key: 'sort',
    text: 'Sort',
    value: 'viewSortDlq',
  },
];
export const summaryLevels = [
  {
    key: '040',
    text: 'State',
    value: '040',
  },
  {
    key: '050',
    text: 'County',
    value: '050',
  },
  {
    key: '160',
    text: 'Place',
    value: '160',
  },
];

export const queueEnvironments = [
  {
    key: 'test',
    text: 'test',
    value: 'test',
  },
  {
    key: 'development',
    text: 'development',
    value: 'development',
  },
  {
    key: 'production',
    text: 'production',
    value: 'production',
  },
];

export const messageTypeOptions = [
  {
    key: 'inbox',
    text: 'inbox',
    value: 'inbox',
  },
  {
    key: 'sort',
    text: 'sort',
    value: 'sort',
  },
  {
    key: 'product',
    text: 'product',
    value: 'product',
  },
];
