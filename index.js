module.exports = ({ service, redisClient, consumer }) => {
  //TODO: When POD restarts, it will have a new name(service).
  //So if there is a consumer list with pending for more than 60 seconds pull the events to active consumer
  //Then if the number of consumer is larger than implied, remove that consumer
  const listenerConfing = ({ listeners }) => {
    listeners.map(({ resolver, eventName, interval = 1000 }) => {
      if (!resolver.name) throw new Error('add name to resolver function! Resolver cannot be an annonymous function.');
      let groupName = service + '-' + resolver.name;
      redisClient.sadd('listenersList', `${service}-${eventName}-${resolver.name}`, err => {
        if (err) console.error(err);
      });
      redisClient.xgroup('CREATE', eventName, groupName, '$', 'MKSTREAM', err => {
        let checkAll = true;
        let xreadgroup = () => {
          // console.log('xreadGroup configured', { checkAll });
          redisClient.xreadgroup(
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
                      let result = await resolver(event);
                      if (result) {
                        //If the resolver successfully consumes the event, remove the event from the group;
                        redisClient.xack(eventName, groupName, ev[0]);
                        // console.log('event consumed!');
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
      redisClient.sadd('eventList', `${service}-${name}`, err => {
        if (err) console.error(err);
      });
      redisClient.xadd(name, '*', 'event', JSON.stringify(body), err => {
        if (err) return reject(err);
        return resolve();
      });
    });
  };

  return { listenerConfing, emitter };
};
