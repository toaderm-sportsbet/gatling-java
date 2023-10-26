package au.com.sportsbet.gatlingjava.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public interface MessageProvider {
    <T> Future<RecordMetadata> sendMessage(String topic, String key, T message);
}
