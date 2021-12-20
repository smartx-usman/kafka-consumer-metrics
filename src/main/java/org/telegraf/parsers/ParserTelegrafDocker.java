package org.telegraf.parsers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.telegraf.datastores.StoreRecordES;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


public class ParserTelegrafDocker implements parsable {
    private static final Logger logger = LogManager.getLogger(ParserTelegrafDocker.class);
    private String ES_INDEX;
    private StoreRecordES store_record_es;

    public ParserTelegrafDocker(String elasticsearch_index) {
        ES_INDEX = elasticsearch_index;
        store_record_es = new StoreRecordES();
    }

    @Override
    public void parse_record(ConsumerRecord<String, String> record) {
        //logger.error(record.value());
        try {
            String[] record_split = record.value().split(" ");

            String measurement_plugin = record_split[0];
            String measurement_values = record_split[1];
            String measurement_timestamp = record_split[2];

            String[] measurement_plugin_labels = measurement_plugin.split(",");
            String[] measurement_value_labels = measurement_values.split(",");

            if (!(measurement_value_labels[0].split("=")[0].equals("memory_total"))) { //ignore memory_total record
                String[] host_label = measurement_plugin_labels[2].split("=");
                String[] server_version_label = measurement_plugin_labels[3].split("=");

                long timestamp_long = Long.parseLong(measurement_timestamp.trim());
                Instant instant = Instant.ofEpochMilli(timestamp_long / 1000000);

                Map<String, Object> jsonMap = new HashMap<>();
                jsonMap.put("@timestamp", instant);
                jsonMap.put(server_version_label[0], server_version_label[1]);
                jsonMap.put(host_label[0], host_label[1]);

                String[] label_and_value;
                for (String measurement_value_label : measurement_value_labels) {
                    label_and_value = measurement_value_label.split("=");

                    if (label_and_value[1].charAt(label_and_value[1].length() - 1) == 'i') {
                        label_and_value[1] = label_and_value[1].substring(0, label_and_value[1].length() - 1);
                    }
                    jsonMap.put(label_and_value[0], label_and_value[1]);
                }

                store_record_es.store_record(ES_INDEX, jsonMap);
            }

        } catch (Exception e) {
            // Throwing an exception
            store_record_es.close_client();
            e.printStackTrace();
        }
    }
}
