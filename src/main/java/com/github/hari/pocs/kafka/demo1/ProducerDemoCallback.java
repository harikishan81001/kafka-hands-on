package com.github.hari.pocs.kafka.demo1;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.logging.ConsoleHandler;

public class ProducerDemoCallback {
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String topicName = "first-topic";
        Integer limit = 1000;

        // Logger
        Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);

        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // Create Consumer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0; i< limit; i++ ) {
            // Create producer record
            String key = new ProducerDemoCallback().generateKey(i);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topicName,
                    key,
                    String.format("Hello Testing Producer: %s", Integer.toString(i)));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info(
                                String.format(
                                        "Received metadata:: Topic: %s, Partition: %s, Key:%s, Offset: %s, Timestamp: %s",
                                        recordMetadata.topic(),
                                        recordMetadata.partition(),
                                        key,
                                        recordMetadata.offset(),
                                        recordMetadata.timestamp()));
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
    private String generateKey(Integer i) {
        Integer keyGen = i % 10;
        String key = String.format("EventKey-%s", keyGen.toString());
        return key;
    }
}
