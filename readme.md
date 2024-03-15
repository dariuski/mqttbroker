## MQTT Broker

MQTT Broker written completely in JavaScript

Features:
  - MQTT v3.1, v5.0
  - access rights for subscribe and publish messages
  - access rights/modifiers for retain, qos flags
  - extensible using modules/plugins

### Usage example

```js
import { Broker } from 'mqtt-broker.js'

const broker = new Broker({
  version: 'MQTT Server v1.0',
  tls: {
    cert: '...',
    key: '...',
    ca: '...'
  },
  listen: "0.0.0.0:1883",
  acl: {
    device: {
      prefix: '$username/$clientId'
    },
    admin: {
      prefix: '',
      permissions: {
        '#': {
          publish: true
        }
      }
    }
  },
  users: {
    device: {
      password: 'mydevice',
      acl: 'device'
    },
    admin: {
      password: 'secret',
      acl: 'admin'
    }
  }
})
```

### License

MIT
