const getRows = async (map, Tasks) => {

  const taskIds = map.map(obj => obj.task_id)

  // id one task type to focus on
  const row = await Tasks.findOne({
    skipLocked: true, order: [['id','ASC']],
    where: { task_id: taskIds, status: 'pending' },
  })
  if(!row) return null

  // find batch of tasks
  const transaction = await Tasks.sequelize.transaction()
  let rows = null
  const task_id = row.task_id
  const limit = map.find(obj => obj.task_id === task_id)?.limit || 1

  try{
    rows = await Que.findAll({
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

const process = function(map, pollMs=5000){

  let isProcessing = false
  const handle = async () => {

    // run sequentially even if its triggered multiple times
    if(isProcessing) return
    isProcessing = true

    // loop over pending rows following FIFO
    while(true){
      const rows = await getRows(map, this)
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

module.exports = process
