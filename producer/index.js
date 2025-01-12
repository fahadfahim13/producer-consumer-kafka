// producer/index.js
const { Kafka } = require('kafkajs');

// Debug logging
console.log('Environment variables:');
console.log('KAFKA_BROKERS:', process.env.KAFKA_BROKERS);
console.log('MESSAGE_COUNT:', process.env.MESSAGE_COUNT);
console.log('BATCH_SIZE:', process.env.BATCH_SIZE);

// Ensure KAFKA_BROKERS is defined
if (!process.env.KAFKA_BROKERS) {
  throw new Error('KAFKA_BROKERS environment variable is not defined');
}

const brokers = process.env.KAFKA_BROKERS.split(',');
console.log('Parsed brokers:', brokers);

const kafka = new Kafka({
  clientId: `producer-${process.pid}`,
  brokers: brokers,
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

const producer = kafka.producer({
  maxInFlightRequests: 100,
  idempotent: true
});

const topic = 'high-volume-topic';
const MESSAGE_COUNT = parseInt(process.env.MESSAGE_COUNT) || 1000000;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 1000;

const generateMessage = (index) => ({
  key: `key-${index}`,
  value: JSON.stringify({
    id: index,
    timestamp: Date.now(),
    data: `Message ${index} from producer ${process.pid}`
  })
});

const produceMessages = async () => {
  try {
    console.log('Connecting to Kafka...');
    await producer.connect();
    console.log('Successfully connected to Kafka');

    const startTime = Date.now();
    let messagesSent = 0;

    while (messagesSent < MESSAGE_COUNT) {
      const batch = [];
      const batchSize = Math.min(BATCH_SIZE, MESSAGE_COUNT - messagesSent);

      for (let i = 0; i < batchSize; i++) {
        batch.push(generateMessage(messagesSent + i));
      }

      try {
        await producer.sendBatch({
          topic,
          messages: batch,
          timeout: 30000,
        });

        messagesSent += batchSize;
        
        if (messagesSent % 10000 === 0) {
          const elapsed = (Date.now() - startTime) / 1000;
          const rate = messagesSent / elapsed;
          console.log(`${messagesSent} messages sent. Rate: ${rate.toFixed(2)} msgs/sec`);
        }
      } catch (error) {
        console.error('Error sending batch:', error);
        throw error;
      }
    }

    const totalTime = (Date.now() - startTime) / 1000;
    console.log(`Finished. Total time: ${totalTime}s, Average rate: ${(MESSAGE_COUNT/totalTime).toFixed(2)} msgs/sec`);

  } catch (error) {
    console.error('Producer error:', error);
    process.exit(1);
  }
};

// Handle process termination
process.on('SIGTERM', async () => {
  try {
    await producer.disconnect();
    process.exit(0);
  } catch (error) {
    console.error('Error during shutdown:', error);
    process.exit(1);
  }
});

console.log('Starting producer...');
produceMessages().catch(console.error);