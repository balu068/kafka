package io.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;


public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    public static void main(String[] args) {
        String groupId = "nhon_test_4";
        String topic = "demo_java";
//Set consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
//Init consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        Thread mainThread = Thread.currentThread();

        Runtime runtime = Runtime.getRuntime();
        runtime.addShutdownHook(new Thread() {
            public void run(){
                log.info("Detect a shutdown...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        //poll for data
        try {
            consumer.subscribe(List.of(topic));

            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "Topic: " + record.partition() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.error("Consumer is shutting down");
        }
        catch (Exception e) {
            log.error("Error ", e);
        } finally {
            consumer.close();
            log.error("Close");
        }

        log.error("asdasdasdasadad");
    }
}