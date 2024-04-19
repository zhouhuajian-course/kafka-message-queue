import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos201:9092,centos202:9092,centos203:9092");
        // 消息键 消息值 String
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送消息
        String userId = "1001";
        String topic = "orders";
        for (int i = 0; i < 3; i++) {
            String message = "User " + userId + " created the order " + (i + 1);
            producer.send(new ProducerRecord<>(topic, userId, message)).get();
        }

        producer.close();
    }
}
