import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
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

    public void create_flink_kafka_consumer(Boolean ExecuteThread, parsable parser_class) {
        try {
            // Displaying the thread that is running
            logger.info("Thread " + Thread.currentThread().getId() + " is running");

            //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //Creating consumer properties
            /*Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
            properties.setProperty("group.id", this.KAFKA_TOPIC + "-group");
            properties.put("enable.auto.commit", "true");
            properties.put("auto.commit.interval.ms", "1000");
            properties.put("session.timeout.ms", "30000");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            // 1.1 set kafka to source
            env.enableCheckpointing(5000); // checkpoint every 5000 msecs

            DataStream<String> stream = env
                    .addSource(new FlinkKafkaConsumer<>(this.KAFKA_TOPIC, new SimpleStringSchema(), properties));*/


            /*KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(this.KAFKA_BOOTSTRAP_SERVERS)
                    .setTopics(this.KAFKA_TOPIC)
                    .setGroupId(this.KAFKA_TOPIC + "-group")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();


            DataStream<String> stream = env
                    .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

            System.out.println(stream.getId());

            System.out.println(stream.print().setParallelism(1));

            env.execute("Kafka Telegraf Metrics Consumer Job");*/
            //stream.process();
            //logger.warn(stream.print());

        } catch (Exception e) {
            // Throwing an exception
            e.printStackTrace();
        }
    }

    public void run() {
        boolean ExecuteThread = true;

        //Ger parser class
        parsable parser_class = get_parser_class();

        if (parser_class == null) {
            ExecuteThread = false;
        }

        //this.create_kafka_consumer(ExecuteThread, parser_class);
        this.create_kafka_consumer(ExecuteThread, parser_class);

    }
}
