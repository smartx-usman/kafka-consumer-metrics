package org.telegraf.parsers;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.telegraf.datastores.StoreRecordES;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ParserLatencyTCP implements parsable {
    private static final Logger logger = LogManager.getLogger(ParserTelegrafSystem.class);

    private final StoreRecordES store_record_es;

    private CollectorRegistry registry = new CollectorRegistry();
    private PushGateway pg = new PushGateway("prometheus-pushgateway.monitoring.svc.cluster.local:9091");

    public ParserLatencyTCP() {
        store_record_es = new StoreRecordES();
    }

    @Override
    public void parse_record(ConsumerRecord<String, String> record, String es_index) {
        try {
            String[] record_split = record.value().split(",");
            logger.error(record.value());
            String[] measurement_timestamp = record_split[0].split(":");
            String[] host_ip = record_split[1].split(":");
            String[] host_name = record_split[2].split(":");
            String[] target_ip = record_split[3].split(":");
            String[] target_name = record_split[4].split(":");
            String[] latency = record_split[5].split(":");

            logger.warn(host_ip[1]);

            Timestamp instant = new Timestamp(System.currentTimeMillis());

            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("@timestamp", instant);
            jsonMap.put(host_ip[0], host_ip[1]);
            jsonMap.put(host_name[0], host_name[1]);
            jsonMap.put(target_ip[0], target_ip[1]);
            jsonMap.put(target_name[0], target_name[1]);
            jsonMap.put(latency[0], latency[1]);

            List<String> labelKeys = Arrays.asList(host_ip[0], host_name[0], target_ip[0], target_name[0]);
            List<String> labelValues = Arrays.asList(latency[0]);

            String jobName = "telegrafJ";
            String metric = "latency";
            String help = "tcp-latency";

            try {
                Gauge counter = Gauge.build()
                        .name(metric)
                        .help(help)
                        .labelNames(labelKeys.toArray(new String[0]))
                        .register(registry);

                counter.labels(labelValues.toArray(new String[0])).inc();
            } finally {
                pg.pushAdd(registry, jobName);
                registry.clear();
            }

            store_record_es.store_record(es_index, jsonMap);
        } catch (Exception e) {
            store_record_es.close_client();
            e.printStackTrace();
        }
    }
}
