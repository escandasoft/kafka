package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaAdvanceLogOffset {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", "127.0.0.1:9092");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Future<RecordMetadata> future = producer.incrementLogOffset("topic", 0);
            System.out.println(future.get(10, TimeUnit.SECONDS));
        }
    }
}
