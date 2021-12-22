package org.telegraf.parsers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.telegraf.datastores.StoreRecordES;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


public class ParserTelegrafK8SPodVolume implements parsable {
    private static final Logger logger = LogManager.getLogger(ParserTelegrafK8SPodVolume.class);
    private String ES_INDEX;
    private StoreRecordES store_record_es;

    public ParserTelegrafK8SPodVolume(String elasticsearch_index) {
        ES_INDEX = elasticsearch_index;
        store_record_es = new StoreRecordES();
    }

    @Override
    public void parse_record(ConsumerRecord<String, String> record) {
        try {
            String[] record_split = record.value().split(" ");

            String measurement_plugin = record_split[0];
            String measurement_values = record_split[1];
            String measurement_timestamp = record_split[2];

            String[] measurement_plugin_labels = measurement_plugin.split(",");
            String[] measurement_value_labels = measurement_values.split(",");

            String[] host_label = measurement_plugin_labels[1].split("=");
            String[] namespace = measurement_plugin_labels[2].split("=");
            String[] pod_name = measurement_plugin_labels[4].split("=");
            String[] volume_name = measurement_plugin_labels[5].split("=");

            long timestamp_long = Long.parseLong(measurement_timestamp.trim());
            Instant instant = Instant.ofEpochMilli(timestamp_long / 1000000);

            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("@timestamp", instant);
            jsonMap.put(namespace[0], namespace[1]);
            jsonMap.put(host_label[0], host_label[1]);
            jsonMap.put(pod_name[0], pod_name[1]);
            jsonMap.put(volume_name[0], volume_name[1]);

            String[] label_and_value;
            for (String measurement_value_label : measurement_value_labels) {
                label_and_value = measurement_value_label.split("=");
                if (label_and_value[1].charAt(label_and_value[1].length() - 1) == 'i') {
                    label_and_value[1] = label_and_value[1].substring(0, label_and_value[1].length() - 1);
                }
                jsonMap.put(label_and_value[0], label_and_value[1]);
            }

            store_record_es.store_record(ES_INDEX, jsonMap);
        } catch (Exception e) {
            store_record_es.close_client();
            e.printStackTrace();
        }
    }
}
