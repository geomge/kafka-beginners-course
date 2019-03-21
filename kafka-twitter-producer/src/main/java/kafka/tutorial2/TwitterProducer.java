package com.geogeorge.kafka.tutorial2;

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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private Logger logger= LoggerFactory.getLogger(TwitterProducer.class);

    //keys are re-generated in twitter to protect the developer account. So the following keys are invalid
    private String consumerKey="BJcK3GooJhKj2ohsqw2Hp0dfZ";
    private String consumerSecret="YDFuG32GF0eEMX6eJmv4VJeTkfrwT44auMgxIC6TLkijBS6jVJ";
    private String token="86780999-LCKBbsShzvdf8HKKa9hUdIFy0GQUAcTZBdIBz1OCu";
    private String secret="ccOTYJfT0Tnn9l1D2JSeZpdf8MzPKy6TYnE4qzu92zw2Q";



    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        //create a twitter client

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Client twitterClient=createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        twitterClient.connect();

        //create a kafka producer
        KafkaProducer<String, String> producer=createkafkaProducer();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook received, stopping application...");
            logger.info("shutting down client from twitter...");
            twitterClient.stop();
            logger.info("closing kafka producer...");
            producer.flush();
            producer.close();
            logger.info("done!");
        }));

        //loop to send tweets to kafka
        while(!twitterClient.isDone()){
            String msg=null;
            try {
                msg=msgQueue.poll(5, TimeUnit.SECONDS);
                if(msg!=null){
                    logger.info("Message is: "+msg);

                    producer.send(new ProducerRecord("twitter_topic", null, msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e==null){
                                logger.info("Received new metadata. \n"+
                                        "Topic: "+recordMetadata.topic()+"\n"+
                                        "Partition: "+recordMetadata.partition()+"\n"+
                                        "Offset: "+recordMetadata.offset()+"\n"+
                                        "Timestamp: "+recordMetadata.timestamp()+"\n");
                            }
                            else{
                                logger.error("Error occurred while producing message", e);
                            }
                        }
                    });
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
                twitterClient.stop();
            }
        }

        logger.info("End of Application");
    }



    private Client createTwitterClient(BlockingQueue<String> msgQueue){


    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

    // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("india", "litmus7", "kochi");
        hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

    private KafkaProducer<String, String> createkafkaProducer(){
        String bootstrapServers = "localhost:9092";

        Properties properties=new Properties();

        //create producer properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all" );
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // Our Kafka 2.0 >= 1.1, so we can set this as 5. Otherwise use 1.
        //above 3 properties are implied in Kafka>=0.11 when idempotence=true. Added for clarity only

        //high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");  //none, snappy, lz4, gzip
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");  //default 0
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));  // 32KB, default 16KB


        //Create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        return producer;
    }

}
