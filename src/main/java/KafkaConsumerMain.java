import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.telegraf.datastores.StoreRecordES;
import org.telegraf.datastores.StoreRecordPrometheus;
import org.telegraf.datastores.Storable;

import java.io.FileReader;
import java.io.IOException;


public class KafkaConsumerMain {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerMain.class);

    private static Storable data_store_class = null;

    public static void main(String[] args) {
        KafkaConsumerMain.LoadConfig(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
    }

    public static void LoadConfig(String kafka_bootstrap_servers, String configFile, String data_store, String es_host, String es_port, String retention_days, String prometheus_pushgateway) {
        if (data_store.equals("elasticsearch"))
            data_store_class = new StoreRecordES(es_host, Integer.parseInt(es_port), retention_days);
        else if (data_store.equals("prometheus"))
            data_store_class = new StoreRecordPrometheus(prometheus_pushgateway);
        else
            logger.error("Invalid data store specified");


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

                consumerThread[i] = new KafkaConsumerThread(kafka_bootstrap_servers, kafka_topic, data_store_class);
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
