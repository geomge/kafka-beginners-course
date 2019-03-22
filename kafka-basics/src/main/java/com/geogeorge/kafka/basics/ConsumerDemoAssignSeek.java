package com.geogeorge.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        String bootstrapServers="localhost:9092";
        //String groupId="my-fourth-application";

        //set consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //earliest/latest/none

        //create consumer
        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topic(s)
        //consumer.subscribe(Collections.singleton("first_topic"));
        //OR        consumer.subscribe(Arrays.asList("first_topic", "second_topic"));

        //assign and seek are normally used to replay messages or fetch a specific message

        //assign
        TopicPartition partitionToReadFrom=new TopicPartition("first_topic", 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        long offsetToReadFrom=10L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead=5;
        int numberOfMessagesReadSoFar=0;
        boolean keepOnReading = true;

        //poll for new data
        while(keepOnReading){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0

            for(ConsumerRecord<String, String> record:records){
                logger.info("Key: "+record.key()+", Value: "+record.value());
                logger.info("Partion: "+record.partition()+", Offset: "+record.offset());
                numberOfMessagesReadSoFar++;
                if(numberOfMessagesReadSoFar>=numberOfMessagesToRead){
                    keepOnReading=false;
                    break;
                }
            }
        }

    }
}
