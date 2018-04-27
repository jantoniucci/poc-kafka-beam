echo LOADING DATA INTO KAFKA
echo -----------------------
echo .
echo - sending sample-input.txt into streams-plaintext-input topic
cat sample-input.txt | docker exec -i pockafkabeam_broker_1  \
    kafka-console-producer \
        --broker-list localhost:9092 \
        --topic streams-plaintext-input

echo .
echo - checking if it was loaded successfully
docker exec pockafkabeam_broker_1 kafka-console-consumer --bootstrap-server localhost:9092 --topic streams-plaintext-input --from-beginning --max-messages 3
echo .
