#!/bin/bash
# Wait for Kafka to be ready
sleep 30

# Create topic automatically
kafka-topics --create --topic afd_data_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

echo "Kafka topic 'afd_data_topic' created successfully"
