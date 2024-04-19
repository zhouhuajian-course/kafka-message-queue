import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo3 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos201:9092,centos202:9092,centos203:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 异步发送 同步发送
        String userId = "1009";
        String topic = "orders";
        String message = "User " + userId + " created an order";
        // 异步发送 业务代码 不需要确保消息发送成功就能继续往下执行 异步确认消息发送情况 性能比较好 可靠性比较差
//        producer.send(new ProducerRecord<>(topic, userId, message), new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                if (e != null) {
//                    // 发送失败
//                    System.out.println(e.getMessage());
//                } else {
//                    // 发送成功
//                    System.out.println(recordMetadata);
//                }
//                System.out.println(Thread.currentThread().getName());
//            }
//        });
        // 同步发送 业务代码  需要确保消息发送成功才能继续往下执行 同步确认消息发送情况 性能比较差 可靠性比较高
        try {
            RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic, userId, message)).get();
            // 发送成功
            System.out.println(recordMetadata);
        } catch (Exception e) {
            // 发送失败
            System.out.println(e.getMessage());
        }

        producer.close();
    }

}
