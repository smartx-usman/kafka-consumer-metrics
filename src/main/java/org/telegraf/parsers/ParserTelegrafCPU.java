package org.telegraf.parsers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.telegraf.datastores.StoreRecordES;
import org.telegraf.datastores.StoreRecordPrometheus;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ParserTelegrafCPU implements parsable {
    private static final Logger logger = LogManager.getLogger(ParserTelegrafCPU.class);

    private final StoreRecordES store_record_es;
    private final StoreRecordPrometheus store_record_prometheus;
    private final File file = new File("/metrics/cpu_usage.csv");
    private final ObjectMapper mapper = new ObjectMapper();
    private FileWriter file_writer = null;
    private SequenceWriter sequence_writer = null;

    public ParserTelegrafCPU(StoreRecordES es, StoreRecordPrometheus prometheus) {
        store_record_es = es;
        store_record_prometheus = prometheus;
        try {
            boolean result = Files.deleteIfExists(file.toPath());
            file_writer = new FileWriter(file, true);
            sequence_writer = mapper.writer().writeValuesAsArray(file_writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void parse_record(ConsumerRecord<String, String> record, String es_index) {
        try {
            String[] record_split = record.value().split(" ");

            String measurement_plugin = record_split[0];
            String measurement_values = record_split[1];
            String measurement_timestamp = record_split[2];

            //logger.warn("measurement_plugin: " + measurement_plugin);

            String[] measurement_plugin_labels = measurement_plugin.split(",");
            String[] measurement_value_labels = measurement_values.split(",");

            String[] cpu_label = measurement_plugin_labels[1].split("=");
            String[] host_label = measurement_plugin_labels[2].split("=");

            long timestamp_long = Long.parseLong(measurement_timestamp.trim());
            Instant instant = Instant.ofEpochMilli(timestamp_long / 1000000);

            Map<String, String> jsonMap = new HashMap<>();
            List<String> labelKeys = Arrays.asList(host_label[0], cpu_label[0]);
            List<String> labelValues = Arrays.asList(host_label[1], cpu_label[1]);

            jsonMap.put("@timestamp", instant.toString());
            jsonMap.put(cpu_label[0], cpu_label[1]);
            jsonMap.put(host_label[0], host_label[1]);

            String[] label_and_value;
            for (String measurement_value_label : measurement_value_labels) {
                label_and_value = measurement_value_label.split("=");
                jsonMap.put(label_and_value[0], label_and_value[1]);

                //store_record_prometheus.store_record(measurement_plugin_labels[0], label_and_value[0], jsonMap, labelKeys, labelValues, label_and_value[1]);
            }

            sequence_writer.write(jsonMap);
            store_record_es.store_record(es_index, jsonMap);
        } catch (Exception e) {
            store_record_es.close_client();
            logger.error("Error in parsing record.", e);
        }
    }

    @Override
    protected void finalize() {
        try {
            file_writer.close();
            sequence_writer.close();
        } catch (IOException e) {
            logger.error("Error in closing file writer.", e);
        }
    }
}
