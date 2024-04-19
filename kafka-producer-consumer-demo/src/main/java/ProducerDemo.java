import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 生产者演示
 */
public class ProducerDemo {
    /**
     * 主方法
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 1. 创建生产者配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "centos201:9092,centos202:9092,centos203:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 2. 创建生产者
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        // 3. 创建消息
        ProducerRecord<Integer, String> record = new ProducerRecord<>("accounts", 2, null, "xiaohong");
        // 4. 发送消息
        producer.send(record).get();
        // 5. 关闭生产者
        producer.close();
    }
}
