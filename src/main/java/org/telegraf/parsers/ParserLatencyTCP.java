package org.telegraf.parsers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.telegraf.datastores.StoreRecordES;
import org.telegraf.datastores.StoreRecordPrometheus;

import java.sql.Timestamp;
import java.util.*;


public class ParserLatencyTCP implements parsable {
    private static final Logger logger = LogManager.getLogger(ParserTelegrafSystem.class);

    private final StoreRecordES store_record_es;
    private final StoreRecordPrometheus store_record_prometheus;

    public ParserLatencyTCP(StoreRecordES es, StoreRecordPrometheus prometheus) {
        store_record_es = es;
        store_record_prometheus = prometheus;
    }

    @Override
    public void parse_record(ConsumerRecord<String, String> record, String es_index) {
        try {
            String[] record_split = record.value().split(",");

            String[] measurement_timestamp = record_split[0].split(":");
            String[] host_ip = record_split[1].split(":");
            String[] host_name = record_split[2].split(":");
            String[] target_ip = record_split[3].split(":");
            String[] target_name = record_split[4].split(":");
            String[] latency = record_split[5].split(":");

            List<String> labelKeys = Arrays.asList(host_ip[0], host_name[0], target_ip[0], target_name[0]);
            List<String> labelValues = Arrays.asList(host_ip[1], host_name[1], target_ip[1], target_name[1]);

            Timestamp instant = new Timestamp(System.currentTimeMillis());

            Map<String, String> jsonMap = new HashMap<>();
            jsonMap.put("@timestamp", instant.toString());
            jsonMap.put(host_ip[0], host_ip[1]);
            jsonMap.put(host_name[0], host_name[1]);
            jsonMap.put(target_ip[0], target_ip[1]);
            jsonMap.put(target_name[0], target_name[1]);
            jsonMap.put(latency[0], latency[1]);

            store_record_prometheus.store_record("latency", latency[0], jsonMap, labelKeys, labelValues, latency[1]);
            store_record_es.store_record(es_index, jsonMap);
        } catch (Exception e) {
            store_record_es.close_client();
            e.printStackTrace();
        }
    }
}
