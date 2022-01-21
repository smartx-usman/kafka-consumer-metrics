import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.telegraf.parsers.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerThread extends Thread {
    private static final Logger logger = (Logger) LogManager.getLogger(KafkaConsumerThread.class);
    private final String KAFKA_BOOTSTRAP_SERVERS;
    private final String KAFKA_TOPIC;
    private final String ES_INDEX;

    public KafkaConsumerThread(String kafka_brokers, String kafka_topic) {
        KAFKA_BOOTSTRAP_SERVERS = kafka_brokers;
        KAFKA_TOPIC = kafka_topic;
        ES_INDEX = KAFKA_TOPIC + "_index";
        logger.info("Kafka Topic --> " + KAFKA_TOPIC);
    }

    public parsable get_parser_class() {
        switch (KAFKA_TOPIC) {
            case "telegraf_cpu":
                return new ParserTelegrafCPU(this.ES_INDEX);
            case "telegraf_disk":
                return new ParserTelegrafDisk(this.ES_INDEX);
            case "telegraf_diskio":
                return new ParserTelegrafDiskio(this.ES_INDEX);
            case "telegraf_docker":
                return new ParserTelegrafDocker(this.ES_INDEX);
            case "telegraf_kubernetes_pod_container":
                return new ParserTelegrafK8SPodContainer(this.ES_INDEX);
            case "telegraf_kubernetes_pod_network":
                return new ParserTelegrafK8SPodNetwork(this.ES_INDEX);
            case "telegraf_kubernetes_pod_volume":
                return new ParserTelegrafK8SPodVolume(this.ES_INDEX);
            case "telegraf_kubernetes_system_container":
                return new ParserTelegrafK8SSystemContainer(this.ES_INDEX);
            case "telegraf_mem":
                return new ParserTelegrafMem(this.ES_INDEX);
            case "telegraf_net":
                return new ParserTelegrafNet(this.ES_INDEX);
            case "telegraf_processes":
                return new ParserTelegrafProcesses(this.ES_INDEX);
            case "telegraf_swap":
                return new ParserTelegrafSwap(this.ES_INDEX);
            case "telegraf_system":
                return new ParserTelegrafSystem(this.ES_INDEX);
            default:
                logger.info("No parser exists for " + KAFKA_TOPIC + " topic. Exiting...");
                break;
        }
        return null;
    }

    public void run() {
        boolean ExecuteThread = true;

        //Ger parser class
        parsable parser_class = get_parser_class();

        if (parser_class == null) {
            ExecuteThread = false;
        }

        try {
            // Displaying the thread that is running
            logger.info("Thread " + Thread.currentThread().getId() + " is running");

            //Creating consumer properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.KAFKA_TOPIC + "-group");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //creating consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

            //Subscribing
            consumer.subscribe(List.of(this.KAFKA_TOPIC));

            //polling
            while (ExecuteThread) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    parser_class.parse_record(record);
                }
            }
        } catch (Exception e) {
            // Throwing an exception
            e.printStackTrace();
        }
    }
}
