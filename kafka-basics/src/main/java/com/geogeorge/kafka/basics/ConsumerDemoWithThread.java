package com.geogeorge.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run(); //implicit constructor
    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        String bootstrapServers="localhost:9092";
        String groupId="my-sixth-application";

        //latch for dealing with multiple threads
        CountDownLatch latch=new CountDownLatch(1);

        logger.info("Creating consumer thread...");
        //creating runnable
        ConsumerRunnable myConsumerRunnable=new ConsumerRunnable(latch, bootstrapServers,groupId );

        //start the thread
        Thread myThread=new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally{
                logger.info("Application has exited");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Thread was interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;

        private Logger logger=LoggerFactory.getLogger(ConsumerRunnable.class);

        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch,
                                String bootstrapServers,
                                String groupId){
            this.latch=latch;

            //set consumer properties
            Properties properties=new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //earliest/latest/none

            //create consumer
            this.consumer=new KafkaConsumer<String, String>(properties);

            //subscribe consumer to our topic(s)
            this.consumer.subscribe(Collections.singleton("first_topic"));
        }

        @Override
        public void run() {
            //poll for new data
            try{
                while(true){
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0

                    for(ConsumerRecord<String, String> record:records){
                        logger.info("Key: "+record.key()+", Value: "+record.value());
                        logger.info("Partion: "+record.partition()+", Offset: "+record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received shutdown signal!");
            }finally{
                consumer.close();

                //tell main code we're done with consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            //wakeup() method is a special method to interrupt consumer.poll()
            //it throws WakeUpException
            consumer.wakeup();
        }
    }
}
