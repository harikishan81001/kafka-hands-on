package twitter;

import com.github.hari.pocs.kafka.demo1.ProducerDemoCallback;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import jdk.nashorn.internal.parser.Token;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    public TwitterProducer(){}
    String CONSUMER_KEY = "SbjEBJmmlE7jkMd80G8gww";
    String CONSUMER_SECRET = "pn0K51ZQoZQ2t9rcRXiQinVjnN6sNouH84BBM2nUGqs";
    String TOKEN = "57883266-guKybdJsRdTDT9rnP8WSpy9U7esetZLXYR1U1R2N1";
    String SECRET = "z4GpUNe1icgDZkdaQLmGwATcwVwgi2GOAy0yBgeKP3Ywa";
    String bootstrapServer = "127.0.0.1:9092";
    String topicName = "twitter-data";
    Integer limit = 1000;

    public void run(){
        Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        Client twClient = twitterClient(msgQueue);
        twClient.connect();
        KafkaProducer<String, String> producer = getProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("shutting down application");
            logger.info("shutting down client applications");
            twClient.stop();
            producer.close();
        }));

        while (!twClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                logger.info(msg);
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, msg);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info(
                                    String.format(
                                            "Received metadata:: Topic: %s, Partition: %s, Offset: %s, Timestamp: %s",
                                            recordMetadata.topic(),
                                            recordMetadata.partition(),
                                            recordMetadata.offset(),
                                            recordMetadata.timestamp()));
                        } else {
                            logger.error("Error while producing", e);
                        }
                    }
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
                twClient.stop();
            }
        }
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public KafkaProducer<String, String> getProducer(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }


    public Client twitterClient(BlockingQueue<String> msgQueue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(Arrays.asList("india", "covid", "covid19"));

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
