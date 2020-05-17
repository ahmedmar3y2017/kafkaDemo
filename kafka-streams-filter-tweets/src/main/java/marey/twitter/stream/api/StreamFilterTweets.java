package marey.twitter.stream.api;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweets {

    public static void main(String[] args) {

        // create properties

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "my_first_group");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        // create topology

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // start filter
        KStream<Object, Object> twitter_tweets = streamsBuilder.stream("twitter_tweets");
        // new filtered
        KStream<Object, Object>  twitter_tweetsFiltered= twitter_tweets.filter((k, v) -> {

            String Id = extractIdFromJson((String) v);
            int followersInTweets = extractUsersFollowersInTweets((String) v);
            if (followersInTweets > 3000) {
                System.out.println("Id : " + Id + " , Followers : " + followersInTweets);

                return true;

            }

            return false;
        });
        // build topology
        twitter_tweetsFiltered.to("important_tweets");

        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        // start stream application
        kafkaStreams.start();


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

    private static String extractIdFromJson(String json) {
        JsonParser jsonParser = new JsonParser();

        return jsonParser.parse(json).getAsJsonObject().get("id_str").getAsString();
    }

}
