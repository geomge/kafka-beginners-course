package com.geogeorge.kafka.consumer.elasticsearch;

import com.google.gson.JsonParser;
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

    public static RestHighLevelClient createClient(){

        String hostname="xxxx";
        String username="xxxx";
        String password="xxxx";

        //Don't do this if you've local ES
        final CredentialsProvider credentialsProvider=new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));

        RestClientBuilder builder= RestClient.builder(new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                                 @Override
                                                 public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                                                     return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                                 }
                                             }
                );

        RestHighLevelClient client=new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServers="localhost:9092";
        String groupId="kafka-demo-elasticsearch";
        //String topic="twitter_topic"; //method parameter

        //set consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //earliest/latest/none
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  //disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");  //limit number of records fetched in a poll

        //create consumer
        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        return  consumer;
    }

    private static String extractIdFromTweet(String tweetJson){
        //gson library
        JsonParser jsonParser=new JsonParser();
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }

    public static void main(String[] args) throws IOException {

        Logger logger=LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client=createClient();

        //create consumer
        KafkaConsumer<String, String> consumer=createConsumer("twitter_topic");

        //poll for new data
        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0
            logger.info("Received "+records.count()+" records.");
            for(ConsumerRecord<String, String> record:records){

                //2 strategies for creating a unique id
                //1. Kafka Generic id
                //String id = record.topic()+"_"+record.partition()+"_"+record.offset();
                //2. Leverage application ID. In this case, get tweet id_str
                String id=extractIdFromTweet(record.value());

                //insert data to elastic search
                IndexRequest indexRequest=new IndexRequest("twitter","tweets", id) //use an id to make our consumer idempotent
                        .source(record.value(), XContentType.JSON);

                try{
                    IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
                    logger.info("Index ID: "+indexResponse.getId());
                } catch (Exception e){
                    logger.warn("Elastic Search API call failed: " + e.getMessage());
                    logger.warn("Skipping bad data: "+record.value());
                }
            }
            logger.info("Committing offsets...");
            consumer.commitSync();
            logger.info("Offsets have been committed!");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //close the client gracefully
        //client.close();
    }


}
