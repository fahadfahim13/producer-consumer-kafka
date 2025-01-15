const { Kafka } = require('kafkajs');

const BATCH_SIZE = 2000; // Adjust based on your needs
const TOTAL_MESSAGES = 1000000;

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: [process.env.KAFKA_BROKER],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

const producer = kafka.producer({
  // Enable idempotent writes to prevent duplicate messages
  maxInFlightRequests: 1,
  idempotent: true,
  // Compression can significantly reduce network bandwidth
  // createPartitioner: Partitioners.LegacyPartitioner,
  // compression: CompressionTypes.GZIP
});

async function sendBatch(messages, topic) {
  try {
    await producer.send({
      topic,
      messages,
      timeout: 30000 // 30 seconds timeout
    });
    console.log(`Sent batch of ${messages.length} messages`);
  } catch (error) {
    console.error('Error sending batch:', error);
    throw error;
  }
}

async function main() {
  try {
    await producer.connect();
    console.log('Producer connected');

    let currentBatch = [];
    let totalSent = 0;

    for (let i = 0; i < TOTAL_MESSAGES; i++) {
      const partition = i % 3;
      currentBatch.push({
        partition,
        value: JSON.stringify({
          id: i,
          message: `Message ${i} from partition ${partition}`,
          timestamp: Date.now()
        })
      });

      // Send batch when it reaches BATCH_SIZE or it's the last iteration
      if (currentBatch.length >= BATCH_SIZE || i === TOTAL_MESSAGES - 1) {
        await sendBatch(currentBatch, process.env.TOPIC_NAME);
        totalSent += currentBatch.length;
        console.log(`Progress: ${totalSent}/${TOTAL_MESSAGES} messages sent`);
        currentBatch = [];
      }
    }
  } catch (error) {
    console.error('Fatal error:', error);
  } finally {
    try {
      await producer.disconnect();
      console.log('Producer disconnected');
    } catch (error) {
      console.error('Error disconnecting:', error);
    }
  }
}

// Handle process termination
['SIGTERM', 'SIGINT', 'SIGQUIT'].forEach(signal => {
  process.on(signal, async () => {
    try {
      await producer.disconnect();
      console.log('Producer disconnected due to', signal);
      process.exit(0);
    } catch (error) {
      console.error('Error disconnecting:', error);
      process.exit(1);
    }
  });
});

main().catch(console.error);