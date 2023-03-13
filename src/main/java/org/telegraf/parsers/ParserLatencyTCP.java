package org.telegraf.parsers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.telegraf.datastores.Storable;
import org.telegraf.datastores.StoreRecordFile;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ParserLatencyTCP implements parsable {
    private static final Logger logger = LogManager.getLogger(ParserTelegrafSystem.class);

    private final Storable data_store_class;
    private final StoreRecordFile store_record_file;

    public ParserLatencyTCP(Storable data_store) {
        data_store_class = data_store;
        store_record_file = new StoreRecordFile("tcp-latency");
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

            //store_record_prometheus.store_record("latency", latency[0], jsonMap, labelKeys, labelValues, latency[1]);
            store_record_file.store_record("tcp-latency", null, jsonMap, null, null, null);
            data_store_class.store_record(es_index, null, jsonMap, null, null, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
