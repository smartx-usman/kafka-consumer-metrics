package org.telegraf.parsers;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.telegraf.datastores.StoreRecordES;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ParserTelegrafSystem implements parsable {
    private static final Logger logger = LogManager.getLogger(ParserTelegrafSystem.class);

    private final StoreRecordES store_record_es;

    private CollectorRegistry registry = new CollectorRegistry();
    private PushGateway pg = new PushGateway("prometheus-pushgateway.monitoring.svc.cluster.local:9091");

    public ParserTelegrafSystem() {
        store_record_es = new StoreRecordES();
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

            String[] measurement_value_labels = measurement_values.split(",");

            logger.warn(host_label[1]);

            Map<String, Object> jsonMap = new HashMap<>();

            if ((measurement_value_labels[0].split("=")[0]).equals("n_users")) {
                String[] label_and_value;
                long timestamp_long = Long.parseLong(measurement_timestamp.trim());
                Instant instant = Instant.ofEpochMilli(timestamp_long / 1000000);

                List<String> labelKeys = Arrays.asList(host_label[0]);
                List<String> labelValues = Arrays.asList(host_label[1]);

                jsonMap.put("@timestamp", instant);
                jsonMap.put(host_label[0], host_label[1]);
                for (String measurement_value_label : measurement_value_labels) {
                    label_and_value = measurement_value_label.split("=");
                    if (label_and_value[1].charAt(label_and_value[1].length() - 1) == 'i') {
                        label_and_value[1] = label_and_value[1].substring(0, label_and_value[1].length() - 1);
                    }
                    jsonMap.put(label_and_value[0], label_and_value[1]);

                    String jobName = "telegrafJ";
                    String metric = label_and_value[0];
                    String help = "host_system_load";

                    try {
                        Gauge counter = Gauge.build()
                                .name(metric)
                                .help(help)
                                .labelNames(labelKeys.toArray(new String[0]))
                                .register(registry);

                        counter.labels(labelValues.toArray(new String[0])).inc();
                    } finally {
                        pg.pushAdd(registry, jobName);
                    }
                }
            }

            store_record_es.store_record(es_index, jsonMap);
        } catch (Exception e) {
            store_record_es.close_client();
            e.printStackTrace();
        }
    }
}
