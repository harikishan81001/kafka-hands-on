package com.github.hari.pocs;

import com.google.gson.JsonParser;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    private static JsonParser jsonParser = new JsonParser();


    public static RestHighLevelClient createESClient(){
        String hostname = "twitter-data-5812827909.us-east-1.bonsaisearch.net";
        String username = "28g9ol0bxo";
        String password = "heoucs7nfk";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        // rest client builder
        RestClientBuilder builder = (
                RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                })
        );
        RestHighLevelClient highLevelClient = new RestHighLevelClient(builder);
        return highLevelClient;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServer = "127.0.0.1:9092";
        Integer limit = 1000;
        String groupId = "kafka-twitter-consumer-group";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(10));
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    private static String extractId(String tweet){
        return jsonParser.parse(tweet).getAsJsonObject().get("id_str").getAsString();
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        String topicName = "twitter-data";
        RestHighLevelClient client = createESClient();

        KafkaConsumer<String, String> kafkaConsumer = createConsumer(topicName);
        while (true){
            ConsumerRecords<String, String> records =  kafkaConsumer.poll(Duration.ofMillis(100));
            logger.info(String.format("Received %s of record", records.count()));
            for (ConsumerRecord<String, String> record: records){
                // making idempotent
                // 2 ways
                // 1. Kafka Generic ID
                String documentId = String.format("%s-%s-%s", record.topic(), record.partition(), record.offset());
                String documentId2 = extractId(record.value());

                logger.info(String.format(
                        "Key: %s, Value: %s, Partition: %s, Offset: %s",
                        record.key(),
                        record.value(),
                        record.partition(),
                        record.offset())
                );

                IndexRequest indexRequest = new IndexRequest("twitter-data", "tweets", documentId2).source(record.value(), XContentType.JSON);
                IndexResponse indexResponse = null;
                indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                logger.info(String.format("Document Id: %s", id));
                Thread.sleep(10);
            }
            logger.info("Committing offset");
            kafkaConsumer.commitSync();
            logger.info("Offset committed successfully!");
            
        }
    }
}
