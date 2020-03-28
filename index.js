module.exports = ({ service, redisClient, numOfReplicas }) => {
  //TODO: When POD restarts, it will have a new name(service).
  //So if there is a consumer list with pending for more than 60 seconds pull the events to active consumer
  //Then if the number of consumer is larger than implied, remove that consumer
  const eventReceiver = ({ receivers }) => {
    receivers.map(({ resolver, stream, consumer }) => {
      if (!resolver.name) throw new Error('add name to resolver function! Resolver cannot be an annonymous function.');
      let groupName = service + '-' + resolver.name;
      redisClient.xgroup('CREATE', stream, groupName, '$', 'MKSTREAM', err => {
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
            stream,
            checkAll ? '0' : '>',
            async (err, data) => {
              // if (err) console.error(err);
              if (data) {
                let events = data[0][1];
                if (events.length === 0) {
                  //if no events are pending listen to new events asynchronously
                  checkAll = false;
                  xreadgroup();
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
                        redisClient.xack(stream, groupName, ev[0]);
                        // console.log('event consumed!');
                      }
                    })
                  );
                  // console.log('events checked!!');
                  xreadgroup();
                }
              } else {
                //keep listening for events
                xreadgroup();
              }
            }
          );
        };
        xreadgroup(); //init event listener
      });
    });
  };

  const eventEmitter = ({ stream, event }) =>
    redisClient.xadd(service + '-' + stream, '*', 'event', JSON.stringify(event));

  return { eventReceiver, eventEmitter };
};
