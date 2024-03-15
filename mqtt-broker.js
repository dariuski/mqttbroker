/**
 * mqtt-broker v1.0.0
 * @copyright Darius Kisonas 2023
 * @license MIT
 */

import { parser as mqttParser, generate as mqttGenerate } from './mqtt-packet.js'
import { EventEmitter } from 'events'
import net from 'net'
import tls from 'tls'
import fs from 'fs'
import os from 'os'

const maxQueueSize = 200

const BROKER_VERSION = 'MQTT-Broker'

const ERROR_UNSPECIFIED = 0x80 // all
const ERROR_NOT_AUTHORIZED = 0x87 // all
const ERROR_MESSAGEID_INUSE = 0x91

/*
supported MQTT features:
  MQTT 3.1,5
  connect: username, password, will, clean, keepalive, sessionExpiryInterval
  publish: qos=0-2, retain
  puback:
  subscribe: qos=0-2, rh (Retain Handling), nl (No Local), rap (Retain as Published)
  unsubscribe:
  acl: permissions, topic prefix, init subscriptions, init publications

TODO: add websocket support
TODO: permissions for subscribe
TODO: implement $SYS/broker/* statistics
*/

/** @description Topics collection
 */
class TopicCollection {
  constructor (options) {
    this.options = options || {}
    this.clear()
  }

  /** Clear all data */
  clear () {
    this.size = 0
    this.all = {}
    this.sub = {}
  }

  /** Add item object to topic */
  add (topic, item) {
    let obj = this, sub = this.all[topic]
    if (!sub) {
      for (const match of topic.matchAll(/(^|\/)([^/]*)/g)) {
        const name = match[2]
        sub = obj.sub && obj.sub[name]
        if (!sub) {
          obj.sub = obj.sub || {}
          sub = obj.sub[name] = {}
        }
        obj = sub
      }
    }
    if (!sub.items) {
      sub.items = []
      sub.topic = topic
      this.all[topic] = sub
    }
    if (item.id) {
      for (let i = 0, n = sub.items.length; i < n; i++) {
        if (sub.items[i].id === item.id) {
          sub.items[i] = item
          return item
        }
      }
    }
    this.size++
    sub.items.push(item)
    return item
  }

  /** Remove item object from topic */
  remove (topic, item) {
    const sub = this.all[topic]
    let result = false
    if (sub && sub.items && sub.items.length) {
      if (item === undefined) {
        result = true
        this.size -= sub.items.length
        sub.items = []
      } else {
        const cb = typeof item === 'function' ? item : obj => obj.id === item
        sub.items = sub.items.filter(obj => {
          const r = cb(obj)
          if (r)
            this.size--
          result |= r
          return !r
        })
      }
    }
    return result
  }

  /** Get first item for topic */
  get (topic) {
    const sub = this.all[topic]
    if (sub && sub.items)
      return sub.items[0]
  }

  /** Set first item for topic */
  set (topic, item) {
    const sub = this.all[topic]
    if (!sub || !sub.items || !sub.items[0])
      this.add(topic, item)
    else
      sub.items[0] = item
    return item
  }

  /** Iterate items using topic wildcard: # - all items and subitems, + - all items the same level */
  iterateWildcard (topic, cb) {
    const sub = this.all[topic]
    const systemFilter = this.options.systemFilter // skip wildcard on $... topics
    function iter (sub, idx) {
      const name = topic[idx]
      if (sub && name) {
        if (name === '#') {
          // iterate all
          if (sub.items && sub.items.some(item => cb(item, sub.topic)))
            return true
        }
        if (sub.sub) {
          if (name === '#' || name === '+') {
            for (let n in sub.sub) {
              if (systemFilter && (idx !== 0 || !n.startsWith('$')))
                if (iter(sub.sub[n], idx + (name === '+' ? 1 : 0)))
                  return true
            }
          } else
            return iter(sub.sub[name], idx + 1)
        }
      }
    }
    if (sub) {
      if (sub.items)
        sub.items.some(item => cb(item, topic))
    } else {
      if (!topic.match(/[+#]/))
        return
      topic = topic.split('/')
      iter(this, 0)
    }
  }

  /** Iterate all items */
  iterate (topic, cb) {
    const internal = this.options.systemFilter && topic.startsWith('$') // skip wildcard on $... topics
    let sub = this.all[topic]
    if (sub) {
      if (sub.items)
        sub.items.some(item => cb(item, topic))
    } else {
      sub = this

      for (const match of topic.matchAll(/(^|\/)([^/]*)/g)) {
        const name = match[2]
        if (sub && sub.sub) {
          let subsub = sub.sub['#']
          if (internal && subsub && sub === this)
            subsub = undefined
          if (subsub && subsub.items && subsub.items.some(item => cb(item, topic)))
            break
          if (internal && sub === this)
            sub = sub.sub[name]
          else
            sub = sub.sub[name] || sub.sub['+']
        } else
          sub = undefined
      }

      if (sub && sub.items)
        sub.items.some(item => cb(item, topic))
    }
  }

  /** Get item[name] value from first matched topic */
  getOption (topic, name, def) {
    let value = def === undefined ? true : def
    this.iterate(topic, option => {
      const v = option[name]
      if (typeof v !== 'undefined') {
        value = v
        return true
      }
    })
    return value
  }
}

/** @description MQTT-Client
 */
export class BrokerClient {
  /** New client */
  constructor (options) {
    Object.assign(this, {
      id: 'client-' + Math.random().toString(16).slice(2),
      username: '',
      auth: false,
      active: true,
      intern: true,
      will: {},
      subscriptions: [],
      queue: [],
      queueIndex: 0,
      qosOutbound: {},
      qosInbound: {},
      timeout: 0,
      messageId: 1,
      prefix: '',
      readTime: new Date(),
      writeTime: new Date(),
      acl: {},
      prefix: ''
    }, options || {})
  }

  get clientId () {
    return this.id
  }

  get address () {
    if (!this._address && this.socket)
      this._address = `${this.socket.remoteAddress}:${this.socket.remotePort}`
    return this._address
  }

  /** cleanup sent packets on successfull communication */
  cleanup (cb) {
    const queue = this.queue
    const removeItem = (idx) => {
      queue.splice(idx, 1)
      if (idx < this.queueIndex)
        this.queueIndex--
      return true
    }

    if (this.queueIndex) {
      if (typeof (cb) === 'number')
        return removeItem(cb)
      if (typeof cb === 'string') {
        const topic = cb
        cb = function (p) { return p.cmd === 'publish' && p.topic === topic }
      }
      if (cb) {
        // remove item by filter
        for (let i = 0, n = queue.length; i < n; i++) {
          if (cb(queue[i], i))
            return removeItem(i)
        }
      } else {
        // cleanup old packets
        let newIdx = 0
        this.queue = queue.filter((packet, idx) => {
          if (idx >= this.queueIndex)
            return true
          const cmd = packet.cmd
          if (packet.messageId) {
            if (cmd === 'publish' || cmd === 'pubrec' || cmd === 'pubrel') {
              newIdx++
              return true
            }
            // removeItem(idx, 1)
          }
        })
        this.queueIndex = newIdx
      }
    }
  }

  /** write packet to the client, for internal use */
  write (packet, cb) {
    if (this.socket)
      this.socket.write(this.parser.generate(packet), cb) //.then(() => cb()).catch(e => cb(e))
    else
      cb(new Error('Invalid client state'))
  }

  /** send packet to the client */
  send (packet) {
    if (packet && !(packet.payload instanceof Buffer) && typeof packet.payload !== 'string')
      packet.payload = packet.payload instanceof Date ? packet.payload.toJSON() : JSON.stringify(packet.payload)

    if (packet && this.queue.length >= maxQueueSize) {
      if (!this.cleanup(packet.topic))
        this.cleanup(0)
    }
    if (packet)
      this.queue.push(packet)
    packet = this.queue[this.queueIndex]
    if (!this._sending && packet) {
      this._sending = true
      this.write(packet, err => {
        this._sending = false
        if (!err) {
          packet.dup = true
          if (this.queue[this.queueIndex] === packet)
            this.queueIndex++
          this.send()
        }
      })
    }
  }

  /** close client connection */
  close () {
    if (this.socket) {
      this.socket.end()
      this.socket = undefined
      return true
    }
  }
}

/** MQTT-Broker server
 */
export class Broker extends EventEmitter {
  /** Start MQTT server
   * @param {Object} options
   * @param {number=60} options.connectionTimeout - client connection timeout in seconds
   * @param {number=60} options.sessionTimeout - client session clean timeout in seconds after connection was closed
   * @param {number=300} options.keepAlive - keep alive (ping) checking in seconds
   * @param {number=200} options.maxPackets - max queued packets pro connection
   * @param {number=1883} options.port - server port (plain: 1883, tls: 8883)
   * @param {Object=} options.tls - tls options
   * @param {string=} options.tls.cert - certificate
   * @param {string=} options.tls.key - certificate key
   * @param {string=} options.tls.ca - ca
   * @param {Object} options.acl - set of acl objects:
   *   {clientId: ['match1', 'match2'], //optional
   *    permissions: {
   *      'topic': {read:true, publish:true, retain:false}
   *    },
   *    prefix: '$clientId|$username'
   *   }
   * @param {Object} options.aclGlobal - global permissions object, Ex:{'topic': {retain:true,read:true,publish:false}}
   * @param {Object|Function} options.users - authorization object: {'username':'password} or function(usr,psw)
   */
  constructor (options) {
    super()
    this.options = {
      sessionTimeout: 60,
      keepAlive: 300,
      connectionTimeout: 60,
      maxPackets: 200, // max number of packets in list
      updateStatistics: 60,
      listen: '',
      tls: {},
      acl: {}, // {default: {prefix:'$username/$clientId', clientId:['prefix'], permissions:{'#':{read:true, publish:false, retain:false}}}
      permissions: {}, // {'topic': { retain:true }}
      users: {}, // {'user': {acl: 'default', password:'psw'}}
      ...options
    }

    this.startupTime = new Date()
    this.protocols = [this.handler]
    this.permissions = new TopicCollection()
    this.subscribers = new TopicCollection({ systemFilter: true })
    this.data = new TopicCollection({ systemFilter: true })
    this.modules = { broker: this }
    this.clients = new Map()

    this.clear()

    this.setOption('permissions', this.options.permissions)
    this.setOption('tls', this.options.tls)

    this.servers = []
    if (this.options.listen)
      this.listen()

    this._ready = false
    this.once('ready', () => this._ready = true)
  }

  /**
   * Add one time listener or call immediatelly for 'module:###', 'ready' 
   * @param {string} name 
   * @param {function} cb 
   */
  once (name, cb) {
    if (name.startsWith('module:') && this.modules[name.slice(7)])
      cb(this.modules[name.slice(7)])
    else if (name === 'ready' && this._ready)
      cb()
    else
      super.once(name, cb)
    return this
  }

  /**
   * Add listener and call immediatelly for 'module:###', 'ready' 
   * @param {string} name 
   * @param {function} cb 
   */
  on (name, cb) {
    if (name.startsWith('module:') && this.modules[name.slice(7)])
      cb(this.modules[name.slice(7)])
    if (name === 'ready' && this._ready)
      cb()
    super.on(name, cb)
    return this
  }

  _listenHandler (socket) {
    const destroy = () => socket.destroy()
    socket.setTimeout(10000, destroy)
    socket.on('error', destroy)
    socket.once('readable', () => {
      if (!socket.encrypted) {
        const chunk = socket.read(1)

        socket.unshift(chunk)
        if (this.options.tls && chunk[0] === 22) {
          socket = new tls.TLSSocket(socket, {
            isServer: true,
            rejectUnauthorized: false,
            secureContext: tls.createSecureContext(this.options.tls)
          })
        } else {
          if (this.options.tls && this.options.tls.required) {
            socket.destroy()
            return
          }
        }
      }
      socket.once('data', data => {
        socket.off('error', destroy)
        let i, n
        for (i = 0, n = this.protocols.length; i < n; i++) {
          if (this.protocols[i].call(this, socket, data))
            break
        }
        if (i === n)
          socket.end()
      })
    })
  }

  /**
   * Start TCP service
   */
  listen (options) {
    this.once('ready', () => {
      let _readyCount = 0
      
      const ready = (srv, err) => {
        if (err) {
          console.error(err.message)
          const i = this.servers.indexOf(srv)
          if (i >= 0)
            this.servers.splice(i, 1)
          srv.close()
        } else
          _readyCount++
        if (_readyCount >= this.servers.length) {
          if (this.servers.length > 0)
            setTimeout(() => this.emit('listen')) // at least 1 server was started
          else
            this.close() // no working servers was created
        }
      }

      let listen = options?.listen
      // Close all listeners
      if (!options) {
        listen = this.options.listen
        this.servers.splice(0).forEach(srv => srv.close())
      } else
        _readyCount = -1
          
      // Start new listeners
      listen.toString().replace(/((\w+):\/\/)?(([^:,]*):)?([^:,]+)/, (_, __, proto, ___, host, port) => {
        const srv = proto === 'tls' || options?.tls?.required
          ? tls.createServer(options?.tls || this.options.tls, (options?.handler || this._listenHandler).bind(this))
          : net.createServer((options?.handler || this._listenHandler).bind(this))
        this.servers.push(srv)
        const _onerr = err => ready(srv, err)
        srv.on('error', _onerr)
        srv.listen(port, host || '0.0.0.0', () => {
          const addr = srv.address()
          console.info('MQTT listen on ' + addr.address + ':' + addr.port)
          ready(srv)
          srv.off('error', _onerr)
        })
      })
    })
  }

  /**
   * Update option
   * @param {string} key 
   * @param {*} value 
   */
  setOption (key, value) {
    // dynamically change option
    switch (key) {
      case 'listen': {
        this.options.listen = value || '1883'
        this.listen()
        break
      }
      case 'restart': {
        if (value) {
          this.close()
          setTimeout(() => os.exit(1), 1000)
        }
        break
      }
      case 'tls': {
        this.options.tls = value
        if (this.options.tls && this.options.tls.cert && this.options.tls.key) {
          if (this.options.tls.cert.indexOf('\n') < 0)
            this.options.tls.cert = fs.readFileSync(this.options.tls.cert)
          if (this.options.tls.key.indexOf('\n') < 0)
            this.options.tls.key = fs.readFileSync(this.options.tls.key)
        } else
          this.options.tls = undefined
        break
      }
      case 'permissions': {
        // remove old permissions
        if (this.permissions.size && this.options.permissions)
          for (let n in this.options.permissions)
            this.permissions.remove(n)
        this.permissions.set('#', { publish: false })
        this.permissions.set('$SYS/#', { publish: false, retain: true })
        this.permissions.set('$SYS/log', { publish: false, retain: false })
        this.permissions.set('$share/+', { publish: false })
        if (typeof value === 'function')
          value = value.call(this)
        value = typeof value === 'object' ? value : {}
        this.options.permissions = { ...value }
        for (let n in value)
          this.permissions.set(n, permissions[n])
        break
      }
      default:
        this.options[key] = value
        break
    }
  }

  log(text) {
    this.broker.publish('$SYS/log', text)
    console.log(text)
  }

  logError(text) {
    this.broker.publish('$SYS/log', text)
    console.error(text)
  }

  logWarn(text) {
    this.broker.publish('$SYS/log', text)
    console.warn(text)
  }

  isClosing () {
    return this._closing
  }

  /** Close server */
  close () {
    if (!this._closing) {
      this._closing = true
      console.info('Closing')
      clearTimeout(this._updateStatistics)
      this.servers.forEach(srv => srv.close())
      this.clear()
      Object.entries(this.modules).forEach(([name, mod]) => {
        if (mod && mod !== this && mod.close)
          mod.close()
      })
      this.emit('close')
    }
  }

  module (name, obj, config) {
    console.info('MODULE ' + name)
    if (typeof obj === 'function')
      obj = obj(this, config) || {}
    else if (obj && obj.plugin)
      obj = obj.plugin(this, config) || obj
    else if (obj && obj.setBroker)
      obj.setBroker(this, config)
    if (obj) {
      this.modules[name] = obj
      this.emit('module', name, obj)
      this.emit('module:' + name, obj)
    }
    return obj
  }

  updateStatistics () {
    if (!this._closing) {
      const stat = this.statistics

      if (stat.clientsMax < this.clients.size)
        stat.clientsMax = this.clients.size

      const data = {
        'version': this.options.version || BROKER_VERSION,
        'clients/connected': this.clients.size - stat.clientsDisconnected,
        'clients/disconnected': stat.clientsDisconnected,
        'clients/total': this.clients.size,
        'clients/maximum': stat.clientsMax,
        'load/bytes/received': stat.bytesReceived,
        'load/bytes/sent': stat.bytesSent,
        'messages/received': stat.messagesReceived,
        'messages/sent': stat.messagesSent,
        'messages/publish/received': stat.publishReceived,
        'messages/publish/sent': stat.publishSent,
        'messages/publish/dropped': stat.publishDropped,
        'messages/retained/count': this.data.size,
        'subscriptions/count': this.subscribers.size,
        'time': Math.floor(new Date() / 1000),
        'uptime': Math.floor(new Date() - this.startupTime)
      }

      for (const n in data)
        this.publish({ topic: '$SYS/broker/' + n, payload: data[n], retain: true })

      this._updateStatistics = clearTimeout(this._updateStatistics)
      if (this.options.updateStatistics > 0)
        this._updateStatistics = setTimeout(() => this.updateStatistics(), this.options.updateStatistics * 1000)
    }
  }

  /** Clear all active connections,subscriptions,publications */
  clear () {
    this.subscribers.clear()
    this.data.clear()
    for (const [id, client] of this.clients.entries()) {
      if (client.socket)
        client.socket.destroy()
    }
    this.clients.clear()

    this.statistics = {
      clientsDisconnected: 0,
      clientsMax: 0,
      bytesReceived: 0,
      bytesSent: 0,
      messagesReceived: 0,
      messagesSent: 0,
      publishReceived: 0,
      publishSent: 0,
      publishDropped: 0
    }
    this.updateStatistics()
  }

  /** Add client to the broker
   * @param {BrokerClient} client - client object
   */
  addClient (client) {
    let oldClient = this.clients.get(client.id)
    if (oldClient)
      oldClient._expire = clearTimeout(oldClient._expire)

    if (client.clean && oldClient) {
      this.removeClient(oldClient)
      oldClient = undefined
    }
    delete client.clean
    this.clients.set(client.id, client)
    if (oldClient) {
      client.subscriptions = oldClient.subscriptions
      client.queue = oldClient.queue
      client.queueIndex = oldClient.queueIndex
      client.qosInbound = oldClient.qosInbound
      client.qosOutbound = oldClient.qosOutbound
      client.messageId = oldClient.messageId
      client.readTime = new Date()
    }

    if (client._connecting) {
      delete client._connecting
      this._clientSend(client, { cmd: 'connack', sessionPresent: !!oldClient, reasonCode: 0, returnCode: 0 })
      if (oldClient)
        return
    }

    if (client.acl) {
      if (client.acl.subscriptions) {
        client.acl.subscriptions.forEach(sub => {
          this.subscribe(sub, client)
        })
      }
      if (client.acl.publications) {
        client.acl.publications.forEach(pub => {
          this.publish(pub, client)
        })
      }
      if (client.acl.globalPermissions) {
        Object.entries(client.acl.globalPermissions).forEach(([n, v]) => {
          if (n.startsWith('/'))
            n.substring(1)
          else
            n = client.prefix + n
          this.permissions.add(n, { ...v, id: client.id })
        })
      }
      if (!(client.acl.permissions instanceof TopicCollection)) {
        const col = new TopicCollection({ systemFilter: true })
        for (let n in client.acl.permissions) {
          const perm = client.acl.permissions[n]
          if (n.startsWith('/'))
            n = n.substring(1)
          else if (client.prefix)
            n = client.prefix + n
          if (n.startsWith('$SYS/'))
            perm.delete('publish') // ignore $SYS publish permissions
          col.add(n, perm)
        }
        client.acl.permissions = col
      }
      //client.keepAlive = this.connectionTimeout / 2
    }
    this.emit('clients/connect', { clientId: client.clientId, username: client.username, address: client.address })
  }

  /**
   * Remove client from the broker
   * @param {BrokerClient} client 
   */
  removeClient (client) {
    client.close()
    client.subscriptions.forEach(topic => this.unsubscribe(topic, client))
    for (const messageId in client.qosInbound)
      this._removeQosInbound(client, messageId)
    for (const messageId in client.qosOutbound)
      this._removeQosOutbound(client, messageId)
    if (client.acl && client.acl.globalPermissions) {
      for (const n in client.acl.globalPermissions) {
        if (n.startsWith('/'))
          n.substring(1)
        else
          n = client.prefix + n
        this.permissions.remove(n, client.id)
      }
    }
    client._expire = clearTimeout(client._expire)
    if (this.clients.get(client.id) === client) {
      this.clients.delete(client.id)
      if (!client.active)
        this.statistics.clientsDisconnected--
    }
  }

  /**
   * If client sends disconnect message
   * @param {BrokerClient} client 
   */
  disconnect (client) {
    if (client.auth && this.clients.get(client.id) === client) {
      console.info('DISCONNECT ' + client.id)
      client.closing = true
      this._close(client)
      this.removeClient(client)
      this.emit('clients/disconnect', { clientId: client.clientId, username: client.username, address: client.address })
    } else
      client.close()
  }

  /** @description Publish topic=payload
   * @param {string} topic - topic
   * @param {string|Buffer} payload - payload
   * @param {boolean} [retain=true] - store publication
   * @param {int} [qos=0] - publication qos
   */
  set (topic, payload, retain, qos) {
    this.publish({
      topic: topic,
      payload: payload,
      retain: typeof retain === 'undefined' ? true : retain,
      qos: qos || 0
    })
  }

  /** @description Get retain publication
   * @param {string} topic
   * @return {Object} publication object: {topic, payload, retain}
   */
  get (topic) {
    return this.data.get(topic)
  }

  /** Update ACL set
   * @param {Object} acl - named acl set
   */
  setAcl (acl) {
    this.options.acl = { ...acl }
    if (!this.options.acl.default)
      this.options.acl.default = { prefix: '$username/$clientId' }
    if (!this.options.acl.admin)
      this.options.acl.admin = { prefix: '', permissions: { '#': { read: true, publish: true } } }
  }

  /** Update users set
   * @param {Object|function} users - users set
   */
  setUsers (users) {
    this.options.users = users
  }

  /** @description Authorize user
   * @param {BrokerClient} client object
   * @param {string} password
   * @throws {Error} if failed
   * @return {BrokerClient} if successfully authorized
   */
  async auth (client, password) {
    let user, acl

    function subOption (obj, name) {
      let option = obj
      name.replace(/[^.]+/g, n => { option = typeof option === 'object' ? option[n] : undefined })
      return option
    }

    if (typeof this.options.users === 'function')
      user = await this.options.users(client.username)
    else
      user = this.options.users[client.username]
    if (user) {
      if (typeof this.options.acl === 'function')
        acl = await this.options.acl(user.acl)
      else
        acl = this.options.acl[user.acl] || this.options.acl.default
      if (acl && typeof acl === 'function')
        acl = await acl(client)
      if (acl && acl.ip && client.socket) {
        function fixIp(addr, mask) {
          if (addr.includes('.'))
            mask = mask ?? 32
            return addr.replace(/.+:/,'').split('.').map(s => {
              const bits = mask > 8 ? 8 : mask
              mask -= bits
              return parseInt(s) & ((1 << bits) - 1)
            }).join('.')
          // IPv6
          addr = addr.split(':')
          const idx = addr.indexOf('')
          if (idx >= 0) {
            addr.splice(idx, 1)
            while (addr.length < 8)
              addr.splice(idx, 0, 0)
          }
          if (addr.length !== 8)
            return 'unknown'
          mask = mask ?? 128
          return addr.map(s => {
            const bits = mask > 16 ? 16 : mask
            mask -= bits
            return (parseInt(s,16) & ((1 << bits) - 1)).toString(16)
          }).join(':')
        }
        const remoteAddress = client.socket.remoteAddress

        if (!acl.ip.find(s => {
            s = s.split('/')
            s[1] = parseInt(s[1] ?? 128)
            return fixIp(s[0], s[1]) === fixIp(remoteAddress, s[1])
          }))
          throw new Error('Access denied')

        const arr = Array.isArray(acl.clientId) ? acl.clientId : [acl.clientId]
        if (!arr.find(v => client.clientId.match(v)))
          throw new Error('Access denied')
      }
      if (acl && acl.clientId) {
        const arr = Array.isArray(acl.clientId) ? acl.clientId : [acl.clientId]
        if (!arr.find(v => client.clientId.match(v)))
          throw new Error('Access denied')
      }

      if (!client.auth && password) {
        if (acl && typeof acl.password === 'function')
          client.auth = await acl.password(client.username, password, client)
        else
          client.auth = password === user.password
      }
      if (client.auth && acl) {
        client.acl = { ...acl }
        client.group = user.group || acl.group
        client.prefix = acl.prefix
          ? (acl.prefix.replace(/\$(\S+)/g, name => subOption(user, name) || subOption(client, name) || '') + '/').replace(/^\/+/, '') : ''
      }
    }
    if (!client.auth)
      throw new Error('Access denied')
    return client
  }

  /**
   * send message to client 
   * @param {BrokerClient} client
   * @param {object} message
  */
  _clientSend (client, message) {
    if (message.cmd === 'publish')
      this.statistics.publishSent++
    this.statistics.messagesSent++
    client.send(message)
  }

  /**
   * handle client connection 
   * @param {BrokerClient} client
   * @param {string} password
  */
  async _connect (client, password) {
    if (!client.auth)
      await this.auth(client, password).catch(() => { })
    console.info('CONNECT ' + client.id + ' ' + client.address + ' auth:' + !!client.auth)
    if (!client.auth) {
      this._clientSend(client, { cmd: 'connack', reasonCode: 134 })
      client.close()
      return
    }
    client.socket.setTimeout((client.keepAlive || 30000) * 5 / 4)
    client.packetIndex = 0
    this.addClient(client)
  }

  /** close client connection */
  _close (client) {
    if (client.close() && client.auth) {
      if (client.sessionTimeout > 0)
        client._expire = setTimeout(() => {
          client._expire = undefined
          this.removeClient(client)
        }, client.sessionTimeout * 1000)

      client.active = false
      if (this.clients.get(client.id) === client)
        this.statistics.clientsDisconnected++
      console.info('CLOSE ' + client.id)

      if (client.will.topic)
        this.publish(client.will, client)

      this.emit('clients/close', { clientId: client.clientId, username: client.username, address: client.address })

      // remove client if no subscriptions
      if (client.subscriptions.length === 0 || client.sessionTimeout === 0)
        this.removeClient(client)
    }
  }

  /** fix client topic */
  _fixTopic (client, topic, permissionId, defaultPermission) {
    let reasonCode = client ? ERROR_NOT_AUTHORIZED : 0
    if (client && topic) {
      let hasPrefix = false
      if (topic.startsWith('/'))
        topic = topic.substring(1)
      else
        if (client.prefix && !topic.startsWith('$')) {
          topic = client.prefix + topic
          hasPrefix = true
        }

      reasonCode =
        (!topic.startsWith('$') ||
          topic.startsWith('$SYS/') ||
          topic.startsWith('$share/') ||
          topic.indexOf('//') < 0) ? 0 : ERROR_NOT_AUTHORIZED
      if (!reasonCode && permissionId && !hasPrefix)
        reasonCode = this._permission(client, topic, permissionId, defaultPermission) ? 0 : ERROR_NOT_AUTHORIZED
    }
    return {
      topic: topic,
      reasonCode: reasonCode
    }
  }


  /** remove qos inbound message for client */
  _removeQosInbound (client, messageId) {
    // qosInbound = {messageId: {topic, payload, qos, clients:{clientId:messageId}}, ...}
    const msg = client.qosInbound[messageId]
    if (msg) {
      for (const clientId in msg.clients) {
        const destMessageId = msg.clients[clientId]
        let destClient = this.clients.get(clientId)
        if (destClient) {
          destClient = dest.qosOutbound[destMessageId]
          if (destClient && destClient.client && destClient.client.clientId === client.clientId)
            delete dest.client
        }
      }
    }
  }

  /** remove qos outbound message for client */
  _removeQosOutbound (client, messageId, reasonCode) {
    // qosOutbound = {messageId: {topic, payload, qos, client:{clientId, messageId}}, ...}
    let msg = client.qosOutbound[messageId]
    if (msg && msg.client) {
      delete client.qosOutbound[messageId]
      const origClient = this.clients.get(msg.client.clientId)
      if (origClient) {
        const origMessageId = msg.messageId
        msg = origClient.qosInbound[origMessageId]
        if (msg)
          delete msg.clients[client.clientId]
        let hasClients
        for (hasClients in msg.clients)
          break
        if (!hasClients && !origClient.closing) {
          this._clientSend(origClient, { cmd: msg.qos === 2 ? 'pubcomp' : 'puback', messageId: origMessageId, reasonCode })
          delete origClient.qosInbound[origMessageId]
        }
      }
    }
  }

  /** Process incomming messages
   * @param {Object} message
   * @param {boolean} intern - used for recursive processing
  */
  process (message, intern) {
    if (intern)
      return true
    if (message.retain) {
      let data = this.data.get(message.topic)
      if (!data)
        data = this.data.set(message.topic, { topic: message.topic, retain: true, qos: 0 })
      if (data.payload === message.payload)
        return false
      data.payload = message.payload
    }
  }

  /** Publish packet
   * @param {Object} packet
   * @param {string} packet.topic - message topic
   * @param {string|Buffer} packet.payload - message payload
   * @param {boolean} [packet.retain=false] - message qos
   * @param {number} [packet.qos=0] - message qos
   * @param {BrokerClient?} client - client or undefined if system message
   */
  publish (packet, client) {
    let topic = packet.topic, reasonCode = 0, retain = packet.retain, payloadString = packet.payload !== undefined ? packet.payload : ''
    if (typeof payloadString !== 'string' && !(payloadString instanceof Buffer))
      payloadString = JSON.stringify(packet.payload)

    // if client is intern module, can publish qos>0, receive only qos=0
    packet.qos = packet.qos || 0

    if (client && !packet.clients) {
      this.statistics.publishReceived++

      ({ topic, reasonCode } = this._fixTopic(client, topic, 'publish'))
      if (!reasonCode)
        retain = this._permission(client, topic, 'retain', retain)
      if (packet.qos === 2 && !client.intern) {
        if (!reasonCode && client.qosInbound[packet.messageId])
          reasonCode = ERROR_MESSAGEID_INUSE
        if (!reasonCode)
          client.qosInbound[packet.messageId] = { topic: topic, payload: packet.payload, qos: packet.qos, retain, messageId: packet.messageId, clients: {} }
        if (reasonCode)
          this.statistics.publishDropped++
        this._clientSend(client, { cmd: 'pubrec', messageId: packet.messageId, reasonCode })
        return
      }
    }

    if (!client) {
      if (!topic.startsWith('$SYS/'))
        console.info('PUBLISH topic:' + topic + ' payload:' + payloadString + ' retain:' + retain)
    } else
      console.info('PUBLISH topic:' + topic + ' payload:' + payloadString + ' reason:' + reasonCode + ' retain:' + retain + ' qos:' + packet.qos + ' from:' + client.id)

    //if (client && reasonCode && packet.qos === 1)
    //  client.send({ cmd: 'puback', messageId: packet.messageId, reasonCode })
    //if (reasonCode)
    //  return

    let qosClients
    const processMessage = { topic: topic, payload: packet.payload, qos: packet.qos || 0, retain }
    if (reasonCode)
      this.statistics.publishDropped++
    if (!reasonCode && this.process(processMessage, packet.intern) !== false) {
      this.emit('publish', processMessage, client)
      this.emit('topic:' + topic, processMessage, client)

      let pubList = {}
      this.subscribers.iterate(topic, sub => {
        const subClient = this.clients.get(sub.id)
        const qos = Math.min(sub.qos, processMessage.qos, subClient.intern ? 0 : 2)
        if (subClient && !subClient.closing && (subClient.active || qos > 0) && !pubList[sub.id] && (!sub.nl || (client && client.id !== sub.id))) {
          const hasPrefix = subClient.prefix && topic.startsWith(subClient.prefix)
          if (hasPrefix || this._permission(subClient, topic, 'read')) { // get all prefixed messages by acl, check acl read permission
            let subtopic = processMessage.topic
            if (hasPrefix)
              subtopic = subtopic.substring(subClient.prefix.length)
            const message = { cmd: 'publish', qos, reference: processMessage.topic, topic: subtopic, payload: payloadString, retain: sub.rap ? packet.retain : false }
            if (sub.cb && sub.cb(message) === false) {
              pubList = {}
              return false
            } else {
              pubList[sub.id] = message
            }
          }
        }
      })
      Object.entries(pubList).forEach(([id, message]) => {
        console.info('> ' + id + ' topic:' + message.topic + ' payload:' + message.payload)
        const subClient = this.clients.get(id)
        this._clientSend(subClient, message)
        if (message.messageId && !client.intern) {
          subClient.qosOutbound[message.messageId] = { qos: message.qos, client: client && { clientId: client.clientId, messageId: packet.messageId } }
          if (!qosClients)
            qosClients = {}
          qosClients[subClient.clientId] = message.messageId
        }
      })
    }

    if (client && !client.intern && packet.qos) {
      if (qosClients)
        client.qosInbound[packet.messageId] = { qos: packet.qos, clients: qosClients }
      else {
        //this._removeQosOutbound(client, packet.messageId, reasonCode)
        delete client.qosInbound[packet.messageId]
        this._clientSend(client, { cmd: packet.qos === 2 ? 'pubcomp' : 'puback', messageId: packet.messageId, reasonCode })
      }
    }
  }

  /** subscribe single client topic
   * @param {Object|string} sub - subscription
   * @param {string} sub.topic - subscription topic (with wildcards: #,+)
   * @param {number} [sub.qos=0] - subscription qos
   * @param {BrokerClient} client - client
   * @param {function} [cb] - optional callback: cb(message) used for modules, if returns false, breaks processing
   */
  subscribe (sub, client, cb) {
    if (typeof sub === 'string')
      sub = { topic: sub }
    const { topic, reasonCode } = this._fixTopic(client, sub.topic, 'read')
    if (reasonCode)
      return reasonCode
    sub.qos = Math.min(client.acl.qos !== undefined ? client.acl.qos : 2, sub.qos || 0)

    console.info('SUB ' + client.id + ' topic:' + sub.topic + ' qos:' + sub.qos + ' rh:' + (sub.rh || 0))

    // resend retain messages
    if (sub.rh !== 2) {
      let process = true
      if (sub.rh === 1)
        this.subscribers.iterate(topic, () => {
          process = false
          return false
        })

      if (process)
        this.data.iterateWildcard(topic, pub => {
          if (pub.retain) {
            let payload = pub.payload

            const hasPrefix = client.prefix && topic.startsWith(client.prefix)
            if (hasPrefix || this._permission(client, topic, 'read')) { // get all prefixed messages by acl, check acl read permission
              let subtopic = pub.topic
              if (hasPrefix)
                subtopic = subtopic.substring(client.prefix.length)

              const message = { cmd: 'publish', qos: Math.min(pub.qos, sub.qos), reference: pub.topic, topic: subtopic, payload: payload, retain: sub.rap }
              if (!cb || cb(message) !== false) {
                console.info('> ' + client.id + ' topic:' + message.topic + ' payload:' + message.payload)
                this._clientSend(client, message)
              }
              if (message.qos > 0 && pub.qos === 1)
                pub.ref++
            }
          }
        })
    }

    this.subscribers.add(topic, { id: client.id, qos: sub.qos, nl: sub.nl, rap: !!sub.rap, cb })
    client.subscriptions.push(sub.topic)
    return sub.qos
  }

  /** unsubscribe single client topic */
  unsubscribe (topic, client) {
    const { topic: subTopic, reasonCode } = this._fixTopic(client, topic)
    console.info('UNSUB ' + client.id + ' topic:' + topic)

    if (!reasonCode && this.subscribers.remove(subTopic, client.id))
      return 0
    return reasonCode || 0x11
  }

  /** get permissions value */
  _permission (client, topic, permissionId, defaultPermission = true) {
    //defaultPermission = typeof defaultPermission === 'undefined' ? true : defaultPermission
    const acl = this.permissions.getOption(topic, permissionId, defaultPermission)
    return (client && client.acl && client.acl.permissions) ? client.acl.permissions.getOption(topic, permissionId, acl) : acl
  }

  /** mqtt protocol handler */
  handler (socket, data) {
    if (data && data[0] !== 0x10) // first packet is allways connect
      return
    const parser = mqttParser()
    parser.generate = (packet) => {
      const buf = mqttGenerate(packet)
      this.statistics.bytesSent += buf.length
      return buf
    }
    const client = new BrokerClient({
      intern: false,
      socket,
      parser
    })

    socket.setTimeout(10000)
    socket.on('close', () => this._close(client))
    socket.on('timeout', () => this._close(client))
    socket.on('error', () => this._close(client))
    socket.on('data', data => {
      this.statistics.bytesReceived += data.length
      client.parser.parse(data)
    })

    client.parser.on('error', () => this._close(client))
    client.parser.on('packet', packet => {
      if (!client.socket)
        return
      // protocol error: connection request not finisched
      if (client._connecting)
        return this._close(client)

      this.statistics.messagesReceived++

      // cleanup send packets as all they where received
      client.cleanup()
      let granted
      switch (packet.cmd) {
        case 'connect': {
          if (client.auth)
            return this._close(client)
          if ((this.clients.get(packet.clientId) || {}).intern)
            return this._close(client) // can't use clientId from intern clients
          Object.assign(client, {
            _connecting: true,
            id: packet.clientId,
            username: packet.username,
            will: packet.will || {},
            keepAlive: (packet.keepalive || 60) * 1000,
            sessionTimeout: packet.sessionExpiryInterval || this.options.sessionTimeout || 30,
            clean: packet.clean
          })
          this._connect(client, packet.password.toString('utf8'))
            .catch(e => {
              console.error(e)
              this._close(client)
            })
          break
        }
        case 'connack':
        case 'suback':
        case 'unsuback':
        case 'pingresp':
        case 'auth':
          // protocol error, invalid message types
          this._close(client)
          break
        case 'pingreq':
          this._clientSend(client, { cmd: 'pingresp' })
          break
        case 'publish':
        case 'pubrel':
          if (packet.cmd === 'pubrel') {
            const messageId = packet.messageId || 0
            packet = client.qosInbound[messageId]
            // check for registered inbound message
            if (!packet) {
              this._clientSend(client, { cmd: 'pubcomp', messageId, reasonCode: ERROR_UNSPECIFIED })
              return
            }
            // inbound message is allready released
            if (!packet.topic) {
              this._clientSend(client, { cmd: 'pubcomp', messageId })
              return
            }
          } else {
            if (packet.payload instanceof Buffer)
              packet.payload = packet.payload.toString('utf8')
            if (typeof packet.payload === 'string' && packet.payload[0] === '{' && packet.payload[packet.payload.length - 1] === '}') {
              try {
                packet.payload = JSON.parse(packet.payload)
              } catch (ignore) { }
            }
            if (typeof packet.payload === 'string' && /^-?\d+(\.\d+)?$/.test(packet.payload))
              packet.payload = parseFloat(packet.payload)
          }
          this.publish(packet, client)
          break
        case 'puback':
          this._removeQosOutbound(client, packet.messageId)
          break
        case 'pubrec':
          client.cleanup(msg => msg.messageId === packet.messageId && msg.cmd === 'publish')
          this._clientSend(client, { cmd: 'pubrel', messageId: packet.messageId })
          break
        case 'subscribe':
          granted = []
          packet.subscriptions.forEach(sub => {
            granted.push(this.subscribe(sub, client))
          })
          this._clientSend(client, { cmd: 'suback', messageId: packet.messageId, granted: granted })
          break
        case 'unsubscribe':
          granted = []
          packet.unsubscriptions.forEach(sub => {
            granted.push(this.unsubscribe(sub, client))
          })
          this._clientSend(client, { cmd: 'unsuback', messageId: packet.messageId, granted: granted })
          break
        case 'disconnect':
          this.disconnect(client)
          break
      }
    })

    if (data)
      client.parser.parse(data)

    return true
  }
}
