import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;


public class KafkaConsumerMain {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerMain.class);

    public static void main(String[] args) {
        KafkaConsumerMain.LoadConfig(args[0], args[1]);
    }

    public static void LoadConfig(String kafka_bootstrap_servers, String configFile) {
        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();

        try (FileReader reader = new FileReader(configFile)) {
            //Read JSON file
            Object obj = jsonParser.parse(reader);
            JSONObject metricsList = (JSONObject) obj;

            JSONArray topics = (JSONArray) metricsList.get("topics");

            // Create a thread array for all topics
            KafkaConsumerThread[] consumerThread = new KafkaConsumerThread[topics.size()];

            // Iterate through each topic and create a thread to process data
            for (int i = 0; i < topics.size(); i++) {
                JSONObject result = (JSONObject) topics.get(i);
                String kafka_topic = result.get("name").toString();

                consumerThread[i] = new KafkaConsumerThread(kafka_bootstrap_servers, kafka_topic);
                consumerThread[i].start();
            }

            // Wait for all threads to finish
            for (KafkaConsumerThread kafkaConsumerThread : consumerThread) {
                kafkaConsumerThread.join();
            }

        } catch (IOException | ParseException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
