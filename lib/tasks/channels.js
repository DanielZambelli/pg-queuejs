const createSubscriber = require('pg-listen')

class Channels{

  client = null

  constructor(pgDbUrl){
    this.client = createSubscriber({ connectionString: pgDbUrl })
    this.connect().then(() => { process.on('exit', this.client.close) })
  }

  connect(){
    return this.client.connect()
  }

  close(){
    return this.client.close()
  }

  listenTo(channel, callback){
    this.client.notifications.on(channel, callback)
    return this.client.listenTo(channel)
  }

  notify(channel, payload){
    return this.client.notify(channel, payload)
  }

  unlistenAll(){
    return this.client.unlistenAll()
  }

  unlisten(channel){
    return this.client.unlisten(channel)
  }

}

module.exports = Channels
