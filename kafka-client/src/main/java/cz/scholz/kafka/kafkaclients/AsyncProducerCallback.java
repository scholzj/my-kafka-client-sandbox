package cz.scholz.kafka.kafkaclients;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsyncProducerCallback implements Callback {
    private final int messageNo;
    private final Boolean debug;
    private AtomicInteger inFlightMessagesCounter;

    public AsyncProducerCallback(int messageNo, Boolean debug, AtomicInteger counter)
    {
        counter.incrementAndGet();
        this.messageNo = messageNo;
        this.debug = debug;
        this.inFlightMessagesCounter = counter;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null)
        {
            inFlightMessagesCounter.decrementAndGet();

            if (debug) {
                System.out.println("-I- confirmed message no. " + messageNo + " to offset " + metadata.offset() + " and partition " + metadata.partition());
            }
        }
        else
        {
            System.out.println("-E- Failed to receive: " + exception.getMessage());
            exception.printStackTrace();
        }
    }
}
