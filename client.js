const { Kafka } = require('kafkajs');

exports.kafka = new Kafka({
  clientId: 'kafka-learn',
  brokers: ['localhost:9092'],
});
