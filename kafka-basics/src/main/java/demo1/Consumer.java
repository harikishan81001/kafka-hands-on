package demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String topicName = "first-topic";
        Integer limit = 1000;
        Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);
        String groupId = "application-1";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topicName));
        while (true){
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records){
                logger.info(String.format(
                        "Key: %s, Value: %s, Partition: %s, Offset: %s",
                        record.key(),
                        record.value(),
                        record.partition(),
                        record.offset())
                );
            }
        }
    }
}
