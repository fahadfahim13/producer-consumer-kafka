const cluster = require('cluster');
const { Kafka } = require('kafkajs');
const numCPUs = require('os').cpus().length;
const fs = require('fs');

if (cluster.isMaster) {
  // Fork workers.
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`);
    cluster.fork(); // Restart the worker
  });
} else {
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
        console.log(`Worker ${process.pid} received message: ${message.value} in topic: ${topic} at partition: ${partition}`);
      const logFilePath = './consumer.log';

      const logMessage = `Worker ${process.pid} received message: ${message.value} in topic: ${topic} at partition: ${partition}\n`;
      fs.appendFile(logFilePath, logMessage, (err) => {
        if (err) {
          console.error(`Failed to write log: ${err}`);
        }
      });
      },
    });
  })();
}