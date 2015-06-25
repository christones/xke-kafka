package fr.xebia.kafka.solutions.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class XNewKafkaProducer implements XKafkaProducer {

    private Properties kafkaProps = new Properties();

    private KafkaProducer<String, String> producer;

    private String topic;

    private String sync;

    public XNewKafkaProducer(String topic) {
        this.topic = topic;
    }

    public void start() {
        producer = new KafkaProducer<String, String>(kafkaProps);
    }

    /* create configuration for the producer
    *  consult Kafka documentation for exact meaning of each configuration parameter */
    public void configure(String brokerList, String sync) {
        this.sync = sync;
        kafkaProps.put("bootstrap.servers", brokerList);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("acks", "1");
    }


    public void produce(final String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, value);
        Future<RecordMetadata> future = producer.send(producerRecord, new KafkaProducerCallback(value));
        if (sync.equals("sync")) {
            future.get();
        }
    }

    public void close() {
        producer.close();
    }

    public class KafkaProducerCallback implements Callback {
        private final String value;

        public KafkaProducerCallback(String record) {
            this.value = record;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null)
                e.printStackTrace();
            System.out.println("The offset of the record " + value + " we just sent is: " + recordMetadata.offset());
        }
    }
}
