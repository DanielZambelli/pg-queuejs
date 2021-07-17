# PG Task Queue
FIFO Task queue in Node.js using PostgreSQL.  

Running resource-intensive tasks, long-running workflows, or batch jobs on web-servers slows down the customer facing service. Instead these types of tasks can be offloaded onto worker-servers.  

Define task callbacks for processing on a worker-process, and trigger it by queueing the task.  

![FIFO](https://github.com/DanielZambelli/pg-task-queue/raw/master/illustration.png "FIFO")

## Install
``` js
npm install pg-task-queue
```

## Initialize
``` js
const PgTaskQue = require('pg-task-queue')

Tasks = new PgTaskQue({
  PG_DB_URL: 'postgresql://admin:root@127.0.0.1/postgres', //required
  DB_POOL_MAX: 5, //defaults to 1
  SYNC_FORCE: false //defaults to false
})
```

## Define tasks available for processing
``` js
Tasks.process([
  { task_id: 'hello_world', callback: (payload) => console.log('Hello World') },
  { task_id: 'hello_name', callback: (payload) => console.log('Hello '+payload.name) },
])
```

## Que a task for processing
``` js
Tasks.que('hello_world', {})
Tasks.que('hello_name', { name: 'Linux' })
Tasks.que('hello_name', [{ name: 'Linux' },{ name: 'Ubuntu' }])
```
