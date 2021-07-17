const Class = require('./index')
const Client = new Class({ PG_DB_URL: process.env.PG_DB_URL })
const sleep = (ms) => new Promise((res) => setTimeout(res, ms))

describe(Client.name, () => {

  afterAll(() => Client.channels.unlistenAll())

  it('is defined', () => {
    expect(Client.defaultTTL).toBeDefined()
    expect(Client.channels).toBeDefined()
    expect(Client.applyTTL).toBeDefined()
    expect(Client.que).toBeDefined()
    expect(Client.subscribe).toBeDefined()
    expect(Client.unsubscribe).toBeDefined()
    expect(Client.completedAll).toBeDefined()
    expect(Client.process).toBeDefined()
  })

  it('que', async () => {
    const task_id = 'test_task_que'
    const payload = { msg: 'hello world' }
    await Client.que(task_id, payload)
    const row = await Client.rows.findOne({ where: { task_id } })
    await Client.rows.destroy({ where: { task_id } })
    expect(row.status).toEqual('pending')
    expect(row.task_id).toEqual(task_id)
    expect(row.payload).toEqual(payload)
  })

  it('que wait completed', async () => {
    const task_id = 'test_task_que_completed'
    const payload = { msg: 'hello world' }
    Client.process([ { task_id, callback: () => null } ])
    const res = await Client.que(task_id, payload, true)
    await Client.rows.destroy({ where: { task_id } })
    expect(res).toBeDefined()
    expect(Array.isArray(res)).toBeTruthy()
    expect(res[0]).toBeDefined()
    expect(Number.isInteger(res[0])).toBeDefined()
  })

  it('que process multiple', async () => {
    const task1 = 'test_task_que_process_multiple_1'
    const task2 = 'test_task_que_process_multiple_2'
    const task3 = 'test_task_que_process_multiple_3'
    const payload = { msg: 'hello world' }
    const res = []
    Client.process([
      { task_id: task1, callback: () => res.push(task1) },
      { task_id: task2, callback: () => res.push(task2) },
      { task_id: task3, callback: () => res.push(task3) },
    ])
    await Client.que(task1, payload)
    await Client.que(task2, payload)
    await Client.que(task1, payload)
    await Client.que(task3, payload)
    await Client.que(task2, payload)
    await Client.que(task1, payload)
    await sleep(100)
    await Client.rows.destroy({ where: { task_id: [task1,task2,task3] } })
    expect(res).toEqual([task1,task2,task1,task3,task2,task1])
  })

  it('subscribe to pending', async () => {
    const task_id = 'test_task_subscribe_pending'
    const payload = { msg: 'hello world' }
    let notification = null
    Client.subscribe(task_id, 'pending', (payload) => notification = payload)
    await Client.que(task_id, payload)
    await Client.rows.destroy({ where: { task_id } })
    await sleep(100)
    expect(notification.task_id).toEqual(task_id)
    expect(notification.status).toEqual('pending')
    expect(notification.id).toBeDefined()
  })

  it('subscribe to completed', async () => {
    const task_id = 'test_task_subscribe_completed'
    const payload = { msg: 'hello world' }
    let notification = null
    Client.subscribe(task_id, 'completed', (payload) => notification = payload)
    Client.process([ { task_id, callback: () => null } ])
    await Client.que(task_id, payload)
    await sleep(100)
    await Client.rows.destroy({ where: { task_id } })
    expect(notification.task_id).toEqual(task_id)
    expect(notification.status).toEqual('completed')
    expect(notification.id).toBeDefined()
  })

  it('subscribe to error', async () => {
    const task_id = 'test_task_subscribe_error'
    const payload = { msg: 'hello world' }
    let notification = null
    Client.subscribe(task_id, 'error', (payload) => notification = payload)
    Client.process([ { task_id, callback: () => { throw new Error('An error occured') } } ])
    await Client.que(task_id, payload)
    await sleep(100)
    await Client.rows.destroy({ where: { task_id } })
    expect(notification.task_id).toEqual(task_id)
    expect(notification.status).toEqual('error')
    expect(notification.id).toBeDefined()
  })

})
