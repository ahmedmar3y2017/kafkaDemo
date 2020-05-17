package com.asset.marey.marey;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(Producer.class);

        String serverBootstrap = "localhost:9092";
        // create producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverBootstrap);
        // type of values you send to kafka
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 5; i++) {

            String value = "Hello from java App " + i;
            String key = "id_" + i;
            String topic = "first_topic";

            // kafka record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);

            // send data -> asyncronius
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        logger.info("recordMetadata : { \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "offset : " + recordMetadata.offset() + "\n" +
                                "TimeStamp : " + recordMetadata.timestamp());
                    } else {
                        // exception
                        logger.error("Exception : " + e.getMessage());
                    }

                }
            }).get();

        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
