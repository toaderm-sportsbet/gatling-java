package au.com.sportsbet.gatlingjava.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MessageService {
    private final MessageProvider messageProvider;

    public MessageService(MessageProvider messageProvider) {
        this.messageProvider = messageProvider;
    }

    public <T> void publishMessage(String topic, T message) {
        try {
            Future<RecordMetadata> future = messageProvider.sendMessage(topic, "key", message);
            RecordMetadata recordMetadata = future.get();
            System.out.println("Message sent to topic " + topic + ", partition " + recordMetadata.partition() +
                    " with offset " + recordMetadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Send failed: " + e.getMessage());
        }
    }

}
