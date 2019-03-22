package com.geogeorge.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger= LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "localhost:9092";

        Properties properties=new Properties();

        //create producer properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);

       for (int i=0; i<5; i++){

           //create producer record
           ProducerRecord<String, String> record=new ProducerRecord<String, String>("first_topic", "id_"+Integer.toString(i),"hello world! "+Integer.toString(i));

           //send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time when message is successfully sent or an exception is thrown
                    if(e==null){
                        logger.info("Received new metadata. \n"+
                                "Topic: "+recordMetadata.topic()+"\n"+
                                "Partition: "+recordMetadata.partition()+"\n"+
                                "Offset: "+recordMetadata.offset()+"\n"+
                                "Timestamp: "+recordMetadata.timestamp()+"\n");
                    }
                    else{
                        logger.error("Error occured while producing message", e);
                    }
                }
            });//.get(); //block the .send to make it synchronous - don't do it in production
        }
        
        //flush data
        producer.flush();

        //flush and close
        producer.close();
    }
}
