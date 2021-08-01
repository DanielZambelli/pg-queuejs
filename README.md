# PG-QueueJS
Queueing jobs in Node.js using PostgreSQL. PG-QueueJS is a job queue built in Node.js on top of PostgreSQL to provide background processing and reliable asynchronous execution to Node.js applications.

## Requirements
* Node 12 or higher
* PostgreSQL 9.5 or higher

## Installation
``` js
npm install pg-queuejs
```

## Initialize
``` js
const PgQue = require('pg-queuejs')

const que = new PgQue({
  pgDbUrl: 'postgresql://admin:root@127.0.0.1/postgres', //required
  dbPoolMax: 1, //defaults to 1
  tasksTTL: '30 days' //rows older than 30 days will be removed. Defaults to '30 days'
})
```

## Subscribe
Subscribe worker process to pending tasks
``` js
que.subscribe([
  { taskId: 'hello-world', callback: (payload) => console.log('Hello World'), batch: 100 },
  { taskId: 'hello-name', callback: (payload) => console.log('Hello '+payload.name), batch: 100 },
  { taskId: 'sum', callback: (payload) => payload.a + payload.b, batch: 100 },
  { taskId: 'cpu-intensive-task', callback: (payload) => cpuIntensiveTask(payload), batch: 1 },
])
```

## Dispatch
Dispatch tasks for processing
``` js
que.dispatch('hello-world', null)
que.dispatch('hello-name', [{ name: 'John Doe' }, { name: 'Jane Doe' }]) //dispatch multiple
await que.dispatch('sum', { a: 2, b: 3 }, true, 1000).then(task => task.result) //awaits result
```


