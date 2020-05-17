package com.asset.marey;

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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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

public class ConsumerElasticSearch {

    static Logger logger = LoggerFactory.getLogger(ConsumerElasticSearch.class);

    public static RestHighLevelClient createClient() {


        String host = "kafka-course-636860616.eu-west-1.bonsaisearch.net";
        String username = "8a95f81ok5";
        String password = "e68bkgt77v";


        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder restClient = RestClient.builder(
                new HttpHost(host, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClient);
        return restHighLevelClient;

    }

    public static KafkaConsumer<String, String> createConsumer(String topicId) {


        String serverBootstrap = "localhost:9092";
        String groupId = "demo_kafka_stream";
        // create Consumer properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverBootstrap);
        // type of values you send to kafka
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "from-beginning");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");

        // create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        // subdcribe consumer to out topics
        kafkaConsumer.subscribe(Arrays.asList(topicId));


        return kafkaConsumer;


    }

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();


        String topic = "important_tweets";

        KafkaConsumer<String, String> kafkaConsumer = createConsumer(topic);

        BulkRequest bulkRequest = new BulkRequest();
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

            int countRecords = consumerRecords.count();
            logger.info("Number Of Records " + countRecords);

            for (ConsumerRecord consumerRecord : consumerRecords) {

                // where you insert into elastic search

                try {

                    String json = (String) consumerRecord.value();
                    String id = extractIdFromJson(json);
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).
                            source(json, XContentType.JSON);
                    // add Bulk Request
                    bulkRequest.add(indexRequest);

                    int followersInTweets = extractUsersFollowersInTweets(json);

                    logger.info("ID is : " + id + " , Followers : " + followersInTweets);


                } catch (NullPointerException e) {
                    logger.error("Skipping bad data : " + consumerRecord.value());

                }
//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                try {
                    Thread.sleep(1000); // small delay
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            if (countRecords > 0) {

                BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                //
                logger.info("Start Commiting ");

                kafkaConsumer.commitAsync();

                // end commiting
                logger.info("End Commiting ");

                try {
                    Thread.sleep(1000); // small delay
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        }


//        client.close();

    }

    private static String extractIdFromJson(String json) {
        JsonParser jsonParser = new JsonParser();

        return jsonParser.parse(json).getAsJsonObject().get("id_str").getAsString();
    }

    private static int extractUsersFollowersInTweets(String json) {
        JsonParser jsonParser = new JsonParser();

        try {
            return jsonParser.parse(json).
                    getAsJsonObject().get("user").
                    getAsJsonObject().get("followers_count").
                    getAsInt();

        } catch (NullPointerException e) {
            return 0;

        }
    }


}
