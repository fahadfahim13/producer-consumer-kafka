const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS]
});

const producer = kafka.producer();
const topic = 'test-topic';

const produceMessage = async () => {
  try {
    await producer.connect();
    let messageCount = 1;

    // Produce a message every 5 seconds
    setInterval(async () => {
      const message = {
        key: `key-${messageCount}`,
        value: `Message ${messageCount} produced at ${new Date().toISOString()}`
      };

      await producer.send({
        topic,
        messages: [message],
      });

      console.log(`Produced message: ${message.value}`);
      messageCount++;
    }, 5000);

  } catch (error) {
    console.error('Error producing message:', error);
  }
};

produceMessage();