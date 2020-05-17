package com.asset.marey.marey;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class Consumer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Consumer.class);
        String serverBootstrap = "localhost:9092";
        String groupId = "my_fifths_application";
        String topic = "first_topic";
        // create Consumer properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverBootstrap);
        // type of values you send to kafka
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        // subdcribe consumer to out topics
//        kafkaConsumer.subscribe(Arrays.asList(topic));


        // assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);

        long offsetReadFrom = 15;
        kafkaConsumer.assign(Arrays.asList(topicPartition));
        // seek
        kafkaConsumer.seek(topicPartition, offsetReadFrom);

        boolean keepReading = true;
        int numberOfMessagingTORead = 5;
        int numberOfMessagingSoFar = 0;
        // pull for new data

        while (keepReading) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord consumerRecord : consumerRecords) {
                numberOfMessagingSoFar++;
                logger.info("Key : " + consumerRecord.key() + " , " + "Value : " + consumerRecord.value());
                logger.info("Partition : " + consumerRecord.partition() + " , " + "Offset : " + consumerRecord.offset());

                if (numberOfMessagingSoFar >= numberOfMessagingTORead) {

                    keepReading = false;
                    break;
                }
            }
        }


    }
}
