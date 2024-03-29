package org.telegraf.parsers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.telegraf.datastores.Storable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


public class ParserTelegrafK8SNode implements parsable {
    private static final Logger logger = LogManager.getLogger(ParserTelegrafK8SNode.class);

    @Override
    public void parse_record(ConsumerRecord<String, String> record, String es_index, Storable data_store_class) {
        try {
            String[] record_split = record.value().split(" ");

            String measurement_plugin = record_split[0];
            String measurement_values = record_split[1];
            String measurement_timestamp = record_split[2];

            String[] measurement_plugin_labels = measurement_plugin.split(",");
            String[] measurement_value_labels = measurement_values.split(",");

            String[] cpu_label = measurement_plugin_labels[1].split("=");
            String[] host_label = measurement_plugin_labels[2].split("=");

            long timestamp_long = Long.parseLong(measurement_timestamp.trim());
            Instant instant = Instant.ofEpochMilli(timestamp_long / 1000000);

            Map<String, String> jsonMap = new HashMap<>();
            jsonMap.put("@timestamp", instant.toString());
            jsonMap.put(cpu_label[0], cpu_label[1]);
            jsonMap.put(host_label[0], host_label[1]);

            String[] label_and_value;
            for (String measurement_value_label : measurement_value_labels) {
                label_and_value = measurement_value_label.split("=");
                jsonMap.put(label_and_value[0], label_and_value[1]);
            }

            data_store_class.store_record(es_index, null, jsonMap, null, null, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
