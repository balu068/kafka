package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Properties;


public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
//Set producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//Init producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//Init producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello_world");
//Send message
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e == null) {
                    log.info("Received new message :\n" +
                            "\ttopic: " + metadata.topic() + "\n" +
                            "\tpartition: " + metadata.partition() + "\n" +
                            "\ttimestamp: " + Instant.ofEpochMilli(metadata.timestamp()).atZone(ZoneOffset.of("+7")) + "\n" +
                            "\toffset: " + metadata.offset()
                    );
                }
                else {
                    log.error("Error while producing " + e);
                }
            }
        });

        producer.flush();
        producer.close();
    }
}