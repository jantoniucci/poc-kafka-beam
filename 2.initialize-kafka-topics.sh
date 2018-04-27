echo INITIALIZE KAFKA TOPICS
echo -----------------------
echo .
echo - Deleting previous topics : fails if not exist
docker exec pockafkabeam_broker_1  \
    kafka-topics --delete \
          --zookeeper zookeeper:2181 \
          --topic streams-plaintext-input

docker exec pockafkabeam_broker_1  \
    kafka-topics --delete \
          --zookeeper zookeeper:2181 \
          --topic streams-wordcount-output

echo - Create the input topic
docker exec pockafkabeam_broker_1  \
    kafka-topics --create \
          --zookeeper zookeeper:2181 \
          --replication-factor 1 \
          --partitions 1 \
          --topic streams-plaintext-input

echo - Create the output topic
docker exec pockafkabeam_broker_1  \
    kafka-topics --create \
          --zookeeper zookeeper:2181 \
          --replication-factor 1 \
          --partitions 1 \
          --topic streams-wordcount-output
