require('dotenv').config()
const Class = require('./index')
let Client = null
const sleep = (ms) => new Promise((res) => setTimeout(res, ms))
const callback = () => null
const poll = 100

describe(Class.name, () => {

  beforeAll(async () => Client = await new Class({ pgDbUrl: process.env.DB_URL }))

  afterAll(() => Client.channels.unlistenAll())

  it('dispatch', async () => {
    const taskId = 'test_dispatch'
    const payload = { msg: 'hello world' }
    await Client.dispatch(taskId, payload)
    const res = await Client.tasks.findOne({ where: { task_id: taskId } })
    await Client.tasks.destroy({ where: { task_id: taskId } })
    expect(res.status).toEqual('pending')
    expect(res.task_id).toEqual(taskId)
    expect(res.payload).toEqual(payload)
  })

  it('dispatch multiple', async () => {
    const taskId = 'test_dispatch_multiple'
    const payload = [{ msg: 'hello world' },{ msg: 'hello world' },{ msg: 'hello world' }]
    await Client.dispatch(taskId, payload)
    const res = await Client.tasks.findAll({ where: { task_id: taskId } })
    await Client.tasks.destroy({ where: { task_id: taskId } })
    expect(res.length).toEqual(3)
    expect(res[0].status).toEqual('pending')
    expect(res[0].payload).toEqual(payload[0])
  })

  it('dispatch awaitCompleted', async () => {
    const taskId = 'test_dispatch_awaitCompleted'
    const payload = 'hello'
    Client.subscribe([ { taskId, callback } ], poll)
    const res = await Client.dispatch(taskId, payload, true, poll)
    await Client.tasks.destroy({ where: { task_id: taskId } })
    expect(res).toBeDefined()
    expect(res.id).toBeDefined()
    expect(res.status).toEqual('completed')
    expect(res.payload).toEqual(payload)
  })

  it('dispatch awaitCompleted multiple', async () => {
    const taskId = 'test_dispatch_awaitCompleted_multiple'
    const payload = [null,null,null]
    Client.subscribe([ { taskId, callback } ], poll)
    const res = await Client.dispatch(taskId, payload, true, poll)
    await Client.tasks.destroy({ where: { task_id: taskId } })
    expect(res.length).toEqual(3)
    expect(res[0].status).toEqual('completed')
    expect(res[0].payload).toEqual(payload[0])
  })

  it('dispatch parallel', async () => {
    const taskId = 'test_dispatch_parallel'
    const payload = taskId
    const timer = setInterval(() => Client.dispatch(taskId, [null,null,null]), poll)
    Client.subscribe([ { taskId, callback } ], poll)
    const res = await Client.dispatch(taskId, payload, true, 1500)
    clearInterval(timer)
    const rows = await Client.tasks.findAll({ where: { task_id: taskId } })
    await Client.tasks.destroy({ where: { task_id: taskId } })
    expect(res.status).toEqual('completed')
    expect(res.payload).toEqual(payload)
    expect(res.id).not.toEqual(rows[0].id)
    expect(res.id).not.toEqual(rows[rows.length-1].id)
  })

  it('dispatch sequence', async () => {
    const task1 = 'test_sequence_1'
    const task2 = 'test_sequence_2'
    const task3 = 'test_sequence_3'
    const payload = 'test_sequence'
    const res = []
    Client.subscribe([
      { taskId: task1, callback: () => res.push(task1) },
      { taskId: task2, callback: () => res.push(task2) },
      { taskId: task3, callback: () => res.push(task3) },
    ], poll)
    await Client.dispatch(task1, payload)
    await Client.dispatch(task2, payload)
    await Client.dispatch(task1, payload)
    await Client.dispatch(task3, payload)
    await Client.dispatch(task2, payload)
    await Client.dispatch(task1, payload)
    await sleep(poll)
    await Client.tasks.destroy({ where: { task_id: [task1,task2,task3] } })
    expect(res).toEqual([task1,task2,task1,task3,task2,task1])
  })

})
