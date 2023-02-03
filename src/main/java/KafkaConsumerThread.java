import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.telegraf.datastores.StoreRecordES;
import org.telegraf.datastores.StoreRecordPrometheus;
import org.telegraf.parsers.*;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class KafkaConsumerThread extends Thread {
    private static final Logger logger = (Logger) LogManager.getLogger(KafkaConsumerThread.class);
    private final String KAFKA_BOOTSTRAP_SERVERS;
    private final String KAFKA_TOPIC;
    private final StoreRecordPrometheus store_record_prometheus;
    private final StoreRecordES store_record_es;
    private final String ES_INDEX;

    public KafkaConsumerThread(String kafka_brokers, String kafka_topic, StoreRecordES es, StoreRecordPrometheus prometheus) {
        KAFKA_BOOTSTRAP_SERVERS = kafka_brokers;
        KAFKA_TOPIC = kafka_topic;
        store_record_prometheus = prometheus;
        store_record_es = es;
        ES_INDEX = KAFKA_TOPIC + "_index";
        logger.info("Kafka Topic --> " + KAFKA_TOPIC);
    }

    public parsable get_parser_class() {
        switch (KAFKA_TOPIC) {
            case "telegraf_cpu":
                return new ParserTelegrafCPU(store_record_es, store_record_prometheus);
            case "telegraf_disk":
                return new ParserTelegrafDisk(store_record_es, store_record_prometheus);
            case "telegraf_diskio":
                return new ParserTelegrafDiskio(store_record_es, store_record_prometheus);
            case "telegraf_docker":
                return new ParserTelegrafDocker(store_record_es, store_record_prometheus);
            case "telegraf_kubernetes_pod_container":
                return new ParserTelegrafK8SPodContainer(store_record_es, store_record_prometheus);
            case "telegraf_kubernetes_pod_network":
                return new ParserTelegrafK8SPodNetwork(store_record_es, store_record_prometheus);
            case "telegraf_kubernetes_pod_volume":
                return new ParserTelegrafK8SPodVolume(store_record_es, store_record_prometheus);
            //case "telegraf_kubernetes_system_container":
            //    return new ParserTelegrafK8SSystemContainer();
            case "telegraf_mem":
                return new ParserTelegrafMem(store_record_es, store_record_prometheus);
            case "telegraf_net":
                return new ParserTelegrafNet(store_record_es, store_record_prometheus);
            //case "telegraf_processes":
            //    return new ParserTelegrafProcesses(store_record_es, store_record_prometheus);
            case "telegraf_swap":
                return new ParserTelegrafSwap(store_record_es, store_record_prometheus);
            case "telegraf_system":
                return new ParserTelegrafSystem(store_record_es, store_record_prometheus);
            case "telegraf_temp":
                return new ParserTelegrafTemp(store_record_es, store_record_prometheus);
            case "telegraf_powerstat_package":
                return new ParserTelegrafPower(store_record_es, store_record_prometheus);
            case "tcp-latency":
                return new ParserLatencyTCP(store_record_es, store_record_prometheus);
            default:
                logger.info("No parser exists for " + KAFKA_TOPIC + " topic. Exiting...");
                break;
        }
        return null;
    }

    public void create_kafka_consumer(Boolean ExecuteThread, parsable parser_class) {
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
            consumer.subscribe(Collections.singleton(this.KAFKA_TOPIC));

            //Date formatting
            String pattern = "yyyy-MM-dd";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

            //polling
            while (ExecuteThread) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    parser_class.parse_record(record, ES_INDEX + "_" + simpleDateFormat.format(new Date()));
                }
            }
        } catch (Exception e) {
            logger.error("Kafka consumer error.", e);
        }
    }


    public void run() {
        boolean ExecuteThread = true;

        //Get parser classes
        parsable parser_class = get_parser_class();

        if (parser_class == null) {
            ExecuteThread = false;
        }

        this.create_kafka_consumer(ExecuteThread, parser_class);
    }
}
