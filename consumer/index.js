const cluster = require('cluster');
const { Kafka, logLevel } = require('kafkajs');
const numCPUs = require('os').cpus().length;
const fs = require('fs/promises');
const path = require('path');

if (cluster.isPrimary) {
  console.log(`Primary ${process.pid} is running`);
  
  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died with code ${code} and signal ${signal}`);
    console.log('Starting a new worker...');
    cluster.fork();
  });
} else {
  const kafka = new Kafka({
    clientId: `consumer-${process.pid}`,
    brokers: [process.env.KAFKA_BROKER],
    logLevel: logLevel.ERROR,
    retry: {
      initialRetryTime: 100,
      retries: 8
    }
  });

  const consumer = kafka.consumer({ 
    groupId: 'test-group',
    maxWaitTimeInMs: 5000,
    maxBytes: 5242880, // 5MB
    sessionTimeout: 45000, // Increased to 45 seconds
    heartbeatInterval: 15000, // Added explicit heartbeat interval
    rebalanceTimeout: 60000, // Added rebalance timeout
    retry: {
      initialRetryTime: 300,
      maxRetryTime: 30000,
      retries: 10
    }
  });

  // Handle graceful shutdown
  const errorTypes = ['unhandledRejection', 'uncaughtException'];
  const signalTraps = ['SIGTERM', 'SIGINT', 'SIGQUIT'];

  errorTypes.forEach(type => {
    process.on(type, async e => {
      try {
        console.log(`process.on ${type}`);
        console.error(e);
        await consumer.disconnect();
        process.exit(0);
      } catch (_) {
        process.exit(1);
      }
    });
  });

  signalTraps.forEach(type => {
    process.once(type, async () => {
      try {
        console.log(`Received ${type} signal. Starting graceful shutdown...`);
        await consumer.disconnect();
        console.log('Consumer disconnected successfully');
      } finally {
        process.kill(process.pid, type);
      }
    });
  });

  async function writeLog(message) {
    const logFilePath = path.join(process.cwd(), 'consumer.log');
    const logMessage = `${new Date().toISOString()} :: Worker ${process.pid} :: ${message}\n`;
    
    try {
      await fs.appendFile(logFilePath, logMessage);
    } catch (err) {
      console.error(`Failed to write log: ${err}`);
    }
  }

  async function processMessage(topic, partition, message) {
    try {
      const messageValue = JSON.parse(message.value.toString());
      const logMessage = `received message: ${JSON.stringify(messageValue)} in topic: ${topic} at partition: ${partition}`;
      await writeLog(logMessage);
      
      // Add your message processing logic here
      
    } catch (error) {
      console.error('Error processing message:', error);
      await writeLog(`Error processing message: ${error.message}`);
      throw error;
    }
  }

  let isShuttingDown = false;

  async function main() {
    await consumer.connect();
    console.log(`Worker ${process.pid} connected to Kafka`);

    // Add consumer rebalance listeners
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      console.log('Consumer joined group:', payload);
    });

    consumer.on(consumer.events.REBALANCING, () => {
      console.log('Consumer group rebalancing...');
    });

    await consumer.subscribe({ 
      topic: process.env.TOPIC_NAME,
      fromBeginning: true
    });

    await consumer.run({
      partitionsConsumedConcurrently: 3, // Reduced from 5 to 3
      eachBatchAutoResolve: false,
      autoCommitInterval: 5000, // Added auto-commit interval
      eachBatch: async ({ 
        batch, 
        resolveOffset, 
        heartbeat,
        isRunning,
        isStale
      }) => {
        const { topic, partition } = batch;
        
        for (let message of batch.messages) {
          if (!isRunning() || isStale() || isShuttingDown) break;
          
          try {
            await processMessage(topic, partition, message);
            resolveOffset(message.offset);
            
            // Perform heartbeat after processing a few messages
            if (message.offset % 3 === 0) {
              await heartbeat();
            }
          } catch (error) {
            console.error(`Error processing message in partition ${partition}:`, error);
            // Implement dead letter queue logic here if needed
            await writeLog(`Failed to process message: ${error.message}`);
          }
        }
      }
    });
  }

  // Improved error handling in main
  main().catch(async error => {
    console.error('Fatal error:', error);
    isShuttingDown = true;
    
    try {
      console.log('Attempting to disconnect consumer...');
      await consumer.disconnect();
      console.log('Consumer disconnected successfully');
    } catch (e) {
      console.error('Error disconnecting consumer:', e);
    }
    
    // Add delay before exit to allow logs to be written
    setTimeout(() => process.exit(1), 1000);
  });
}