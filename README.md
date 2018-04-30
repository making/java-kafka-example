# Apache Kafka example for Java

Example code for connecting to a Apache Kafka cluster. 

## Running locally

If you just want to test it out.

### Configure

```
export KAFKA_BROKERS=broker1:9094,broker2:9094,broker3:9094
```

### Run

```
git clone 
cd java-kafka-example
mvn clean compile assembly:single
java -jar target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar
```

This will start a Java application that pushes messages to Kafka in one Thread and read messages in the main Thread. 
The output you will see in the terminal is the messages received in the consumer.
