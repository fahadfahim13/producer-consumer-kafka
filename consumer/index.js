// consumer/index.js
const { Kafka } = require('kafkajs');

// Debug logging
console.log('Environment variables:');
console.log('KAFKA_BROKERS:', process.env.KAFKA_BROKERS);
console.log('CONSUMER_GROUP_ID:', process.env.CONSUMER_GROUP_ID);

// Ensure KAFKA_BROKERS is defined
if (!process.env.KAFKA_BROKERS) {
  throw new Error('KAFKA_BROKERS environment variable is not defined');
}

const brokers = process.env.KAFKA_BROKERS.split(',');
console.log('Parsed brokers:', brokers);

const kafka = new Kafka({
  clientId: `consumer-${process.pid}`,
  brokers: brokers,
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

const consumer = kafka.consumer({ 
  groupId: process.env.CONSUMER_GROUP_ID || 'high-volume-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

const topic = 'high-volume-topic';

const consumeMessages = async () => {
  try {
    console.log('Connecting to Kafka...');
    await consumer.connect();
    console.log('Successfully connected to Kafka');

    console.log(`Subscribing to topic: ${topic}`);
    await consumer.subscribe({ 
      topic,
      fromBeginning: true
    });
    console.log('Successfully subscribed to topic');

    let messageCount = 0;
    const startTime = Date.now();

    await consumer.run({
      autoCommit: true,
      autoCommitInterval: 5000,
      autoCommitThreshold: 100,
      partitionsConsumedConcurrently: 3,
      eachBatch: async ({ batch, heartbeat, isRunning, isStale }) => {
        if (!isRunning() || isStale()) return;

        console.log(`Received batch of ${batch.messages.length} messages from partition ${batch.partition}`);
        
        for (const message of batch.messages) {
          if (!isRunning() || isStale()) break;

          messageCount++;
          
          try {
            // Parse and process message
            const value = JSON.parse(message.value.toString());
            
            if (messageCount % 10000 === 0) {
              const elapsed = (Date.now() - startTime) / 1000;
              const rate = messageCount / elapsed;
              console.log(`Consumer ${process.pid}: Processed ${messageCount} messages. Rate: ${rate.toFixed(2)} msgs/sec`);
              console.log('Sample message:', value);
            }

            await heartbeat();
          } catch (error) {
            console.error('Error processing message:', error);
            // Continue processing other messages even if one fails
          }
        }
      }
    });

  } catch (error) {
    console.error('Consumer error:', error);
    process.exit(1);
  }
};

// Handle process termination
process.on('SIGTERM', async () => {
  try {
    console.log('Disconnecting consumer...');
    await consumer.disconnect();
    process.exit(0);
  } catch (error) {
    console.error('Error during shutdown:', error);
    process.exit(1);
  }
});

// Handle errors
consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
  console.log('Consumer joined group:', payload);
});

consumer.on(consumer.events.CRASH, ({ payload }) => {
  console.error('Consumer crashed:', payload);
  process.exit(1);
});

console.log('Starting consumer...');
consumeMessages().catch(console.error);