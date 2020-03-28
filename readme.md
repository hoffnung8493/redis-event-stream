# redis-event-stream

\*\*Simplified redis5 stream client for event driven architecture.

[![npm version](https://img.shields.io/npm/v/redis-stream.svg)](https://www.npmjs.com/package/redis-event-stream)

## Installation

```shell
npm install redis-event-stream
```

## Examples

### Configuration & Event Receiver

```js
const { redisClient } = require('./redis'); //setup redis connection first
const eventMaster = require('redis-event-stream');

const sampleReceiver = async event => {
  console.log(`feedbackSeen Event Received!!`);
  console.log(JSON.stringify(event, undefined, 2));
  return true;
  //After the resolver consumes the event, it must return true.
  //Then that event will be removed from the stream group consumer list
};

const { eventReceiver, eventEmitter } = eventMaster({ service: 'SERVICE_NAME', numOfReplicas: 1, redisClient });

eventReceiver({
  receivers: [{ resolver: sampleReceiver, stream: 'EVENT_NAME', consumer: 'INSTANCE_ID' }]
});
```

### Event Emitter

```js
const { eventReceiver, eventEmitter } = eventMaster({ service: 'SERVICE_NAME', numOfReplicas: 1, redisClient });

//after some business logic
let eventBody = { foo: bar };

eventEmitter({ stream: 'EVENT_NAME', event: eventBody });
```

## Contributing

If you find a bug or want to propose a feature, refer to [the issues page](https://github.com/hoffnung8493/redis-event-stream/issues).
