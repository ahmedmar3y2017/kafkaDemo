package marey.twitter;

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

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public TwitterProducer() {
    }

    //"consumerKey", "consumerSecret", "token", "secret"
    String consumerKey = "wi7Ap157RCAgSbpCLpTqK9L7c";
    String consumerSecret = "PB1LoPHbP7psG6TgtUSRY8oDqPChBQwW0HN8R5PcsA3BQe3ETc";
    String token = "3632838196-WTCCaxnaU3dcSNHsidasGRsw2fb5TknrfF1ydP6";
    String secret = "yPLZ0KE5S9IvZHXnenqMPHWPv8cEF4XF0NNN7z5Sf76gI";

    public static void main(String[] args) {

        new TwitterProducer().run();

    }


    public void run() {

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create twitter client
        Client twitterClient = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        twitterClient.connect();

        // create kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        // add shutdown Hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            logger.info("Stopping application ......");
            // close twitter client
            logger.info("Shutting down client from twitter ......");
            twitterClient.stop();
            // close kafka producer
            logger.info("Closing producer ......");
            kafkaProducer.close();
            logger.info("Done ......");


        }));
        // loop to send tweets

        while (!twitterClient.isDone()) {
            String message = null;
            try {
                message = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();

            }
            if (message != null) {
                logger.info(message);
                // kafka record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("twitter_tweets", null, message);

                // send data -> asyncronius
                kafkaProducer.send(producerRecord, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Exception : ", e);


                        }
//                        if (e == null) {
//                            logger.info("recordMetadata : { \n" +
//                                    "Topic: " + recordMetadata.topic() + "\n" +
//                                    "Partition : " + recordMetadata.partition() + "\n" +
//                                    "offset : " + recordMetadata.offset() + "\n" +
//                                    "TimeStamp : " + recordMetadata.timestamp());
//                        } else {
//                            // exception
//                            logger.error("Exception : " + e.getMessage());
//                        }

                    }
                });

            }

        }
        logger.info("End Application :");

    }

    private KafkaProducer<String, String> createKafkaProducer() {

        String serverBootstrap = "localhost:9092";
        // create producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverBootstrap);
        // type of values you send to kafka
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // safe producer
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // Hight throuput producer

        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");

        // create producer
        final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        return kafkaProducer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("bitcoin");
//        hosebirdEndpoint.followings(followings);
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

}
