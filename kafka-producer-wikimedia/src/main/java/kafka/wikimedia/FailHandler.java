package kafka.wikimedia;

import jakarta.ws.rs.sse.InboundSseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class FailHandler implements Consumer<Throwable> {
    private static final Logger log = LoggerFactory.getLogger(FailHandler.class.getSimpleName());
    @Override
    public void accept(Throwable throwable) {
        log.error(throwable.toString());
    }
}
