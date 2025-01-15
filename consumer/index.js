const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: [process.env.KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: 'test-group' });

(async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: process.env.TOPIC_NAME, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received message: ${message.value} in topic: ${topic} at partition: ${partition}`);
    },
  });
})();