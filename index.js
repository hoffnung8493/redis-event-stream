module.exports = ({ service, redisClient, consumer, devMode = true }) => {
  //TODO: When POD restarts, it will have a new name(service).
  //So if there is a consumer list with pending for more than 60 seconds pull the events to active consumer
  //Then if the number of consumer is larger than implied, remove that consumer
  if (!redisClient) throw new Error('redisClient not provided!!!');
  const listenerConfing = ({ listeners }) => {
    let list = listeners.map(v => ({ name: v.resolver.name, eventName: v.eventName }));
    for (let listener of list) {
      if (list.filter(v => v.name === listener.name && v.eventName === listener.eventName) > 1)
        throw new Error('DUPLICATED!! [resolver name, eventName] pair must be unique ');
    }
    listeners.map(({ resolver, eventName, interval = 1000 }) => {
      if (!resolver.name) throw new Error('add name to resolver function! Resolver cannot be an annonymous function.');
      let groupName = service + '-' + resolver.name;
      let client = redisClient.duplicate();
      client.sadd('listenersList', `${service}-${eventName}-${resolver.name}`, err => {
        if (err) console.error(err);
      });
      client.xgroup('CREATE', eventName, groupName, '$', 'MKSTREAM', err => {
        let checkAll = true;
        let xreadgroup = () => {
          // console.log('xreadGroup configured', { checkAll });
          client.xreadgroup(
            'GROUP',
            groupName,
            consumer,
            'COUNT',
            10,
            'BLOCK',
            10000,
            'STREAMS',
            eventName,
            checkAll ? '0' : '>',
            async (err, data) => {
              if (err) throw err;
              if (data) {
                let events = data[0][1];
                if (events.length === 0) {
                  //if no events are pending listen to new events asynchronously
                  checkAll = false;
                  setTimeout(() => xreadgroup(), interval);
                } else {
                  //while consuming events, new events might be published.
                  //check for non-consumed events -> checkAll = true;
                  // console.log({ data });
                  checkAll = true;
                  await Promise.all(
                    events.map(async ev => {
                      currentId = ev[0];
                      let event = JSON.parse(ev[1][1]);
                      if (devMode) console.log(`RECEIVED EVENT<${eventName}>, body: ${JSON.stringify(event)}`);
                      let result = await resolver(event);
                      if (result) {
                        //If the resolver successfully consumes the event, remove the event from the group;
                        client.xack(eventName, groupName, ev[0]);
                        if (devMode) console.log('EVENT consumed!');
                      }
                    })
                  );
                  // console.log('events checked!!');
                  setTimeout(() => xreadgroup(), interval);
                }
              } else {
                //keep listening for events
                setTimeout(() => xreadgroup(), interval);
              }
            }
          );
        };
        setTimeout(() => xreadgroup(), interval); //init event listener
      });
    });
  };

  const emitter = ({ name, body }) => {
    return new Promise((resolve, reject) => {
      try {
        client = redisClient.duplicate();
        client.sadd('eventList', `${service}-${name}`, err => {
          if (err) console.error(err);
          client.xadd(name, '*', 'event', JSON.stringify(body), err => {
            if (devMode) console.log(`PUBLISHED EVENT <${name}>, body: ${JSON.stringify(body)}`);
            if (err) return reject(err);
            resolve();
          });
        });
      } catch (err) {
        console.error(err);
      }
    });
  };

  return { listenerConfing, emitter };
};
