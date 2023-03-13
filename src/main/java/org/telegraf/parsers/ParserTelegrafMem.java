package org.telegraf.parsers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.telegraf.datastores.Storable;
import org.telegraf.datastores.StoreRecordFile;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


public class ParserTelegrafMem implements parsable {
    private static final Logger logger = LogManager.getLogger(ParserTelegrafMem.class);

    private final Storable data_store_class;
    private final StoreRecordFile store_record_file;

    public ParserTelegrafMem(Storable data_store) {
        data_store_class = data_store;
        store_record_file = new StoreRecordFile("memory_usage");
    }

    @Override
    public void parse_record(ConsumerRecord<String, String> record, String es_index) {
        try {
            String[] record_split = record.value().split(" ");

            String measurement_plugin = record_split[0];
            String measurement_values = record_split[1];
            String measurement_timestamp = record_split[2];

            String[] measurement_plugin_labels = measurement_plugin.split(",");
            String[] measurement_value_labels = measurement_values.split(",");

            String[] host_label = measurement_plugin_labels[1].split("=");

            long timestamp_long = Long.parseLong(measurement_timestamp.trim());
            Instant instant = Instant.ofEpochMilli(timestamp_long / 1000000);

            Map<String, String> jsonMap = new HashMap<>();
            jsonMap.put("@timestamp", instant.toString());
            jsonMap.put(host_label[0], host_label[1]);

            String[] label_and_value;
            for (String measurement_value_label : measurement_value_labels) {
                label_and_value = measurement_value_label.split("=");
                if (label_and_value[1].charAt(label_and_value[1].length() - 1) == 'i') {
                    label_and_value[1] = label_and_value[1].substring(0, label_and_value[1].length() - 1);
                }
                jsonMap.put(label_and_value[0], label_and_value[1]);
            }

            store_record_file.store_record(measurement_plugin_labels[0], null, jsonMap, null, null, null);
            data_store_class.store_record(es_index, null, jsonMap, null, null, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
