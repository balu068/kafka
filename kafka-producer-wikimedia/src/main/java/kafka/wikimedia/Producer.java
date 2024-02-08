package kafka.wikimedia;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.sse.SseEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "demo_java";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        Thread mainThread = Thread.currentThread();

        Runtime runtime = Runtime.getRuntime();
        runtime.addShutdownHook(new Thread() {
            public void run(){
                log.info("Detect a shutdown...");
                producer.flush();
                producer.close();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        final Client client = ClientBuilder.newClient();
        final WebTarget target = client.target(url);
        try {
            final SseEventSource source = SseEventSource
                    .target(target)
                    .reconnectingEvery(500, TimeUnit.MILLISECONDS)
                    .build();
            source.register(new SuccessHandler(topic, producer), new FailHandler());
            source.open();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    }
