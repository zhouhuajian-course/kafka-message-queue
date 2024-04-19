import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        // 1. 创建消费者配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos201:9092,centos202:9092,centos203:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order_system");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 2. 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 3. 订阅主题
        consumer.subscribe(Collections.singleton("orders"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("onPartitionsRevoked " + collection);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("onPartitionsAssigned " + collection);
            }
        });
        // 4. 拉取消息 消费消息
        boolean isRunning = true;
        while (isRunning) {
            try {
                // 拉取消息
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                // 消费消息
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
                }

            } catch (Exception e) {
                System.out.println(e.getMessage());
                isRunning = false;
            }
        }
        // 5. 关闭消费者
        consumer.close();
    }
}
