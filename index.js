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
          console.log('xreadGroup configured', { checkAll });
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
              if (err) console.error(err);
              if (data) {
                let events = data[0][1];
                if (events.length === 0) {
                  //처리 안된 이벤트가 없으면 비동기적으로 새로운 이벤트를 대기한다.
                  checkAll = false;
                  xreadgroup();
                } else {
                  //이벤트 처리하는 동안 새로운 이벤트가 들어왔을 수도 있다.
                  //이벤트 처리 후 처리 안된 모든 이벤트를 확인한다.
                  console.log({ data });
                  checkAll = true;
                  await Promise.all(
                    events.map(async ev => {
                      currentId = ev[0];
                      let event = JSON.parse(ev[1][1]);
                      let result = await resolver(event);
                      if (result) {
                        //이벤트 처리 성공 -> 그룹 리스트에서 제거한다.
                        redisClient.xack(stream, groupName, ev[0]);
                        console.log('event consumed!');
                      }
                    })
                  );
                  console.log('events checked!!');
                  xreadgroup();
                }
              } else {
                //계속 이벤트를 기다린다.
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
