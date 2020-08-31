const TinyQueue = require('tinyqueue');

let queue = new TinyQueue([], function (a, b) {
  return a.coalesce - b.coalesce;
});
queue.push({ coalesce: 5, arr: [1, 2, 3] });
queue.push({ coalesce: 6, arr: [1, 2, 3] });
queue.push({ coalesce: 1, arr: [1, 2, 3] });
queue.push({ coalesce: 4, arr: [1, 2, 3] });
queue.push({ coalesce: 3, arr: [1, 2, 3] });
queue.push({ coalesce: 9, arr: [1, 2, 3] });
queue.push({ coalesce: 8, arr: [1, 2, 3] });
queue.push({ coalesce: 2, arr: [1, 2, 3] });
queue.push({ coalesce: 7, arr: [1, 2, 3] });

console.log(queue.data);

while (queue.length) {
  const item = queue.pop();
  console.log(item);
}
