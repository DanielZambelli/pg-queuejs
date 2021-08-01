const { Sequelize, Op, Model, DataTypes: { JSONB, INTEGER, STRING } } = require('sequelize')
const Channels = require('./channels')

class PgQueueJS{

  channels=null
  db=null
  tasks=null
  tasksTTL=null

  constructor({
    pgDbUrl=null,
    dbPoolMax=1,
    tasksTTL='30 days',
  }){

    if(!pgDbUrl) throw new Error('pgDbUrl missing in PgQueueJS constructor')

    this.channels = new Channels(pgDbUrl)
    this.db = this._initDb(pgDbUrl, dbPoolMax)
    this.tasks = this._initTasks()
    this.tasksTTL = tasksTTL

    return Promise.resolve()
      .then(() => this.tasks.sync())
      .then(() => this._applyTTL())
      .then(() => this)
  }

  subscribe(taskDefinitions, pollInterval=15000){
    const process = this._process(taskDefinitions)
    this.channels.listenTo('pgqueuejs-work-pending', process)
    setInterval(process, pollInterval)
    process()
  }

  async dispatch(taskId, payloads, awaitCompleted=false, awaitCompletedInterval=5000){
    if(!Array.isArray(payloads)) payloads = [payloads]
    payloads = payloads.map(payload => ({ task_id: taskId, payload }))
    const tasks = await this.tasks.bulkCreate(payloads)
    await this.channels.notify('pgqueuejs-work-pending')
    const ids = tasks.map(row => row.id)
    if(awaitCompleted) {
      await this._completed(taskId, ids, awaitCompletedInterval)
      const method = payloads.length > 1 ? 'findAll' : 'findOne'
      return await this.tasks[method]({ where: { id: ids } })
    }
    return ids
  }

  _initDb(url, max){
    return new Sequelize(url, {
      logging: false, dialect: 'postgres',
      dialectOptions: { dateStrings: true, },
      pool: { min: 0, max, idle: 30000, acquire: 10000 },
    })
  }

  _initTasks(){
    class Tasks extends Model{}
    Tasks.init({
      id: { type: INTEGER, primaryKey: true, autoIncrement: true },
      status: { type: STRING, allowNull: false, defaultValue: 'pending', validate: { isIn: [['pending','processing','completed','error']] } },
      task_id: { type: STRING, allowNull: false },
      payload: { type: JSONB },
      result: { type: JSONB, },
      log: { type: JSONB },
    },{
      sequelize: this.db,
      modelName: 'tasks',
      freezeTableName: true,
    })
    return Tasks
  }

  _applyTTL(ttl = this.tasksTTL){
    if(!ttl) return
    const [amount,unit] = ttl.split(' ')
    const recent = require('moment')().subtract(amount, unit).format('YYYY-MM-DD hh:mm:ss')
    return this.tasks.destroy({ where: { createdAt: { [Op.lte]: recent } }})
  }

  async _completed(taskIds, rowIds=null, pollInterval=5000){
    const SQL = { limit: 1, where: { task_id: taskIds, status: ['pending','processing'] } }
    if(rowIds) SQL.where.id = rowIds
    // poll as long as there are uncompleted tasks
    while( await this.tasks.count(SQL).then(count => count > 0) ){
      await new Promise(res => setTimeout(res, pollInterval))
    }
  }

  _process(taskDefinitions){
    let isProcessing = false
    return async () => {
      // run sequentially even if its triggered multiple times
      if(isProcessing) return
      isProcessing = true

      // loop over pending rows following FIFO
      while(true){
        const tasks = await this._getTasksForProcessing(taskDefinitions)
        if(tasks === -1) continue
        if(!tasks) break

        // invoke callback
        await Promise.all(tasks.map(task => {
          return new Promise(async (resolve) => {
            try{
              task.result = await taskDefinitions.find(definition => definition.taskId === task.task_id).callback(task.payload)
              task.status = 'completed'
            }catch(err){
              task.log = err
              task.status = 'error'
            }finally{
              await task.save()
              return resolve()
            }
          })
        }))
      }
      isProcessing = false
    }
  }

  async _getTasksForProcessing(taskDefinitions){

    const taskIds = taskDefinitions.map(definition => definition.taskId)

    // id one task type to focus on
    const row = await this.tasks.findOne({
      skipLocked: true, order: [['id','ASC']],
      where: { task_id: taskIds, status: 'pending' },
    })
    if(!row) return null

    // find batch of tasks
    const transaction = await this.db.transaction()
    let rows = null
    const task_id = row.task_id
    const limit = taskDefinitions.find(definition => definition.taskId === task_id).batch || 1

    try{
      rows = await this.tasks.findAll({
        transaction, lock: true, skipLocked: true,
        where: { task_id, status: 'pending' }, order: [['id','ASC']], limit
      })
      if(!rows.length) {
        await transaction.rollback()
        return null
      }

      for(const row of rows){
        row.status = 'processing'
        await row.save({ transaction })
      }
      await transaction.commit()

    }catch(error){
      await transaction.rollback()
      if(rows){
        for(const row of rows){
          row.status = 'error'
          row.log = error.toString()
          await row.save()
        }
        return -1
      }
    }
    return rows
  }

}

module.exports = PgQueueJS
