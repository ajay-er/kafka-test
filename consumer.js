const { kafka } = require('./client');

async function init() {
  const consumer = kafka.consumer({ groupId: 'user-1', fromBeginning: true });

  console.log('Connecting consumer');
  await consumer.connect();

  await consumer.subscribe({ topic: 'rider-updates' });
  await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      try {
        const decodedMessage = JSON.parse(message.value.toString());
        console.log(`[${topic}]: PART:${partition}:`, decodedMessage);
      } catch (error) {
        console.error('Error decoding message:', error);
      }
    },
  });

  //   consumer.disconnect();
}

init();
