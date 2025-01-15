const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: [process.env.KAFKA_BROKER],
});

const producer = kafka.producer();

(async () => {
  await producer.connect();
  for (let i = 0; i < 1000000; i++) {
    await producer.send({
      topic: process.env.TOPIC_NAME,
      messages: [{ value: `Message ${i}` }],
    });
    console.log(`Sent message ${i}`);
  }
  await producer.disconnect();
})();