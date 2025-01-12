const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS]
});

const consumer = kafka.consumer({ groupId: 'test-group' });
const topic = 'test-topic';

const consumeMessages = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          topic,
          partition,
          offset: message.offset,
          value: message.value.toString(),
          timestamp: new Date(parseInt(message.timestamp)).toISOString()
        });
      },
    });
  } catch (error) {
    console.error('Error consuming message:', error);
  }
};

consumeMessages();