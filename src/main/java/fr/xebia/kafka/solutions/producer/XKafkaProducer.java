package fr.xebia.kafka.solutions.producer;

import java.util.concurrent.ExecutionException;

public interface XKafkaProducer {

    /**
     * Create configuration for the producer.
     * See http://kafka.apache.org/documentation.html#producerconfigs for old producers
     * See http://kafka.apache.org/documentation.html#newproducerconfigs for new producers
     * @param brokerList
     * @param sync
     */
    void configure(String brokerList, String sync);

    /**
     * Start the producer
     */
    void start();

    /**
     * Create record and send to Kafka.
     * Because the key is null, data will be sent to a random partition.
     * The producer will switch to a different random partition every 10 minutes.
     *
     * @param value the value of the record
     * @throws ExecutionException
     * @throws InterruptedException
     */
    void produce(String value) throws ExecutionException, InterruptedException;

    /**
     * Stop/close the producer.
     */
    void close();
}
