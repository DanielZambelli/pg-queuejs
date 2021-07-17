const Sequelize = require('sequelize')
const { Op, Model, DataTypes: { JSONB, INTEGER, STRING }} = require('sequelize')
const Channels = require('./channels')

class PgTaskQue{

  constructor({
    PG_DB_URL=null,
    DB_POOL_MAX=5,
    SYNC_FORCE=false
  }){

    if(!PG_DB_URL) throw new Error('PG_DB_URL is required in TasksJs constructor')

    this.db = new Sequelize(PG_DB_URL, {
      logging: false,
      dialect: 'postgres',
      dialectOptions: { dateStrings: true, },
      pool: { min: 0, max: DB_POOL_MAX, idle: 30000, acquire: 10000 },
    })

    this.channels = new Channels(PG_DB_URL)

    class Tasks extends Model{}
    Tasks.init({
      id: { type: INTEGER, primaryKey: true, autoIncrement: true },
      status: { type: STRING, allowNull: false, defaultValue: 'pending', validate: { isIn: [['pending','processing','completed','error']] } },
      task_id: { type: STRING, allowNull: false },
      payload: { type: JSONB, allowNull: false, defaultValue: {} },
      log: { type: STRING },
    },{
      sequelize: this.db,
      modelName: 'tasks',
      freezeTableName: true,
    })
    this.rows = Tasks
    this.rows.sync({ force: SYNC_FORCE }).then(() => this.applyTTL())
  }

  db=null
  channels=null
  rows=null
  defaultTTL = '30 days'

  applyTTL(ttl = this.defaultTTL){
    if(!ttl) return
    const [amount,unit] = ttl.split(' ')
    const recent = require('moment')().subtract(amount, unit).format('YYYY-MM-DD hh:mm:ss')
    return this.rows.destroy({ where: { createdAt: { [Op.lte]: recent } }})
  }

  async que(task_id, payloads, awaitCompleted=false, awaitCompletedMs=1000){
    if(payloads === null) throw new Error('expected payload of object or array of objects')
    if(!Array.isArray(payloads)) payloads = [payloads]
    const tasks = await this.rows.bulkCreate( payloads.map(payload => ({ task_id, payload })) )
    const status = 'pending'
    const ids = tasks.map(task => task.id)
    for(const id of ids){ await this.channels.notify(task_id, { task_id, status, id }) }
    if(awaitCompleted) await this.completedAll(task_id, awaitCompletedMs)
    return ids
  }

  async completedAll(taskIds, ms=5000){
    const SQL = { where: { task_id: taskIds, status: ['pending','processing'] } }
    let row = null
    while(row = await this.rows.findOne(SQL)){
      if(!row) break
      await new Promise(res => setTimeout(res, ms))
    }
  }

  subscribe(task_id, status, callback){
    this.channels.listenTo(task_id, async (notification) => {
      if(status !== notification.status) return
      callback(notification)
    })
  }

  unsubscribe(task_id){
    return this.channels.unlisten(task_id)
  }

  async _getRows(map){

    const taskIds = map.map(obj => obj.task_id)

    // id one task type to focus on
    const row = await this.rows.findOne({
      skipLocked: true, order: [['id','ASC']],
      where: { task_id: taskIds, status: 'pending' },
    })
    if(!row) return null

    // find batch of tasks
    const transaction = await this.db.transaction()
    let rows = null
    const task_id = row.task_id
    const limit = map.find(obj => obj.task_id === task_id)?.limit || 1

    try{
      rows = await this.rows.findAll({
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

  process(map, pollMs=5000){

    let isProcessing = false
    const handle = async () => {

      // run sequentially even if its triggered multiple times
      if(isProcessing) return
      isProcessing = true

      // loop over pending rows following FIFO
      while(true){
        const rows = await this._getRows(map)
        if(rows === -1) continue
        if(!rows) break

        // invoke task callback
        await Promise.all(rows.map(async row => {
          try{
            await map.find(obj => obj.task_id === row.task_id).callback(row.payload)
            row.status = 'completed'
          }catch(err){
            row.status = 'error'
            row.log = err.toString()
          }finally{
            await row.save()
            await this.channels.notify(row.task_id, { task_id: row.task_id, status: row.status, id: row.id })
          }
        }))
      }
      await this.applyTTL()
      isProcessing = false
    }

    // subscribe, poll and check for new work to do
    const taskIds = map.map(obj => obj.task_id)
    taskIds.forEach(task_id => this.subscribe(task_id, 'pending', handle))
    setInterval(handle, pollMs)
    handle()
  }
}

module.exports = PgTaskQue
