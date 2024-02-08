package kafka.wikimedia;

import jakarta.ws.rs.sse.InboundSseEvent;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.function.Consumer;

public class SuccessHandler implements Consumer<InboundSseEvent> {
    String topic;
    KafkaProducer<String, String> producer;
    public  SuccessHandler(String topic, KafkaProducer<String, String> producer){
        this.topic = topic;
        this.producer = producer;
    };
    private static final Logger log = LoggerFactory.getLogger(SuccessHandler.class.getSimpleName());
    @Override
    public void accept(InboundSseEvent event) {
        String messageData = event.readData(String.class);
        if (messageData != null) {
            producer.send(new ProducerRecord<>(topic, messageData), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info("Received new message :\n" +
                                "\ttopic: " + metadata.topic() + "\n" +
                                "\tpartition: " + metadata.partition() + "\n" +
                                "\ttimestamp: " + Instant.ofEpochMilli(metadata.timestamp()).atZone(ZoneOffset.of("+7")) + "\n" +
                                "\toffset: " + metadata.offset()
                        );
                    }
                    else {
                        log.error("Error while producing " + exception);
                    }
                }
            });
            producer.flush();
        }
    }
}
