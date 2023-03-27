import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.telegraf.datastores.Storable;
import org.telegraf.datastores.StoreRecordES;
import org.telegraf.datastores.StoreRecordFile;
import org.telegraf.datastores.StoreRecordPrometheus;
import org.telegraf.parsers.*;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Calendar;
import java.util.Properties;

public class KafkaConsumerThread extends Thread {
    private static final Logger logger = (Logger) LogManager.getLogger(KafkaConsumerThread.class);
    private final String KAFKA_BOOTSTRAP_SERVERS;
    private final String KAFKA_TOPIC;
    private Storable data_store_class = null;
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
                return new ParserTelegrafCPU();
            case "telegraf_disk":
                return new ParserTelegrafDisk();
            case "telegraf_diskio":
                return new ParserTelegrafDiskio();
            case "telegraf_docker":
                return new ParserTelegrafDocker();
            case "telegraf_kubernetes_daemonset":
                return new ParserTelegrafK8SDaemonset();
            case "telegraf_kubernetes_deployment":
                return new ParserTelegrafK8SDeployment();
            case "telegraf_kubernetes_service":
                return new ParserTelegrafK8SService();
            case "telegraf_kubernetes_statefulset":
                return new ParserTelegrafK8SStatefulset();
            case "telegraf_kubernetes_pod_container":
                return new ParserTelegrafK8SPodContainer();
            case "telegraf_kubernetes_pod_network":
                return new ParserTelegrafK8SPodNetwork();
            case "telegraf_kubernetes_pod_volume":
                return new ParserTelegrafK8SPodVolume();
            //case "telegraf_kubernetes_system_container":
            //    return new ParserTelegrafK8SSystemContainer();
            case "telegraf_mem":
                return new ParserTelegrafMem();
            case "telegraf_net":
                return new ParserTelegrafNet();
            //case "telegraf_processes":
            //    return new ParserTelegrafProcesses();
            case "telegraf_swap":
                return new ParserTelegrafSwap();
            case "telegraf_system":
                return new ParserTelegrafSystem();
            case "telegraf_temp":
                return new ParserTelegrafTemp();
            case "telegraf_powerstat_package":
                return new ParserTelegrafPower();
            case "tcp-latency":
                return new ParserLatencyTCP();
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

            String data_store = System.getenv("DATA_STORE");
            String es_host = System.getenv("ES_HOSTNAME");
            String es_port = System.getenv("ES_PORT");
            String retention_days = System.getenv("ES_INDEX_RETENTION_DAYS");
            String prometheus_pushgateway = System.getenv("PROMETHEUS_PUSHGATEWAY");

            //Date formatting
            String previous_date = "", current_date, pattern = "yyyy-MM-dd-HH";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
            Calendar calendar = Calendar.getInstance();

            switch (data_store) {
                case "elasticsearch":
                    data_store_class = new StoreRecordES(es_host, Integer.parseInt(es_port), retention_days);
                    break;
                case "prometheus":
                    data_store_class = new StoreRecordPrometheus(prometheus_pushgateway);
                    break;
                case "file":
                    data_store_class = new StoreRecordFile(ES_INDEX + "_" + simpleDateFormat.format(calendar.getTime()));
                    break;
                default:
                    logger.error("Invalid data store specified.");
                    System.exit(0);
            }

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

            //polling
            while (ExecuteThread) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                current_date = simpleDateFormat.format(calendar.getTime());

                if (!previous_date.equals(current_date)) {
                    data_store_class.createRecordFile(ES_INDEX + "_" + current_date);
                    previous_date = current_date;
                }

                for (ConsumerRecord<String, String> record : records) {
                    parser_class.parse_record(record, ES_INDEX + "_" + simpleDateFormat.format(calendar.getTime()), data_store_class);
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
