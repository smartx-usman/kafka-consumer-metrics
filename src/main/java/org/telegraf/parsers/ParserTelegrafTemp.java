package org.telegraf.parsers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.telegraf.datastores.Storable;
import org.telegraf.datastores.StoreRecordFile;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ParserTelegrafTemp implements parsable {
    private static final Logger logger = LogManager.getLogger(ParserTelegrafTemp.class);

    private final Storable data_store_class;
    private final StoreRecordFile store_record_file;

    public ParserTelegrafTemp(Storable data_store) {
        data_store_class = data_store;
        store_record_file = new StoreRecordFile("temperature");
    }

    @Override
    public void parse_record(ConsumerRecord<String, String> record, String es_index) {
        try {
            String[] record_split = record.value().split(" ");

            String measurement_plugin = record_split[0];
            String measurement_values = record_split[1];
            String measurement_timestamp = record_split[2];

            String[] measurement_plugin_labels = measurement_plugin.split(",");
            String[] host_label = measurement_plugin_labels[1].split("=");
            String[] sensor_label = measurement_plugin_labels[2].split("=");

            String[] measurement_value_labels = measurement_values.split("=");

            Map<String, String> jsonMap = new HashMap<String, String>();

            List<String> labelKeys = Collections.singletonList(host_label[0]);
            List<String> labelValues = Collections.singletonList(host_label[1]);

            if (measurement_value_labels[0].contains("temp")) {
                long timestamp_long = Long.parseLong(measurement_timestamp.trim());
                Instant instant = Instant.ofEpochMilli(timestamp_long / 1000000);

                jsonMap.put("@timestamp", instant.toString());
                jsonMap.put(host_label[0], host_label[1]);
                jsonMap.put(sensor_label[0], sensor_label[1]);
                jsonMap.put(measurement_value_labels[0], measurement_value_labels[1]);

                store_record_file.store_record(measurement_plugin_labels[0], null, jsonMap, null, null, null);
                data_store_class.store_record(es_index, null, jsonMap, null, null, null);
                //store_record_prometheus.store_record(measurement_plugin_labels[0], label_and_value[0], jsonMap, labelKeys, labelValues, label_and_value[1]);
            }
        } catch (Exception e) {
            logger.error("Error in parsing record.", e);
        }
    }
}
