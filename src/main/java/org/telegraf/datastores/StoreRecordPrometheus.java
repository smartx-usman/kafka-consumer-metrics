package org.telegraf.datastores;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StoreRecordPrometheus implements Storable {
    private static final Logger logger = LogManager.getLogger(StoreRecordPrometheus.class);
    private final CollectorRegistry registry = new CollectorRegistry();
    private final PushGateway push_gateway;

    public StoreRecordPrometheus(String push_gateway) {
        this.push_gateway = new PushGateway(push_gateway);
    }

    @Override
    public void store_record(String plugin, String metric, Map<String, String> record, List<String> labelKeys, List<String> labelValues, String measurement) {
        String jobName = "telegraf-" + plugin + "-metrics";
        Map<String, String> grouping_key = new HashMap<String, String>();
        grouping_key.put("job", jobName);
        grouping_key.put("instance", "aida");

        for (int i = 0; i < labelKeys.size(); i++) {
            grouping_key.put(labelKeys.get(i), labelValues.get(i));
        }

        try {
            Gauge gauge = Gauge.build()
                    .name(metric)
                    .help(metric + " measurement data.")
                    .labelNames(labelKeys.toArray(new String[0]))
                    .register(registry);

            gauge.labels(labelValues.toArray(new String[0])).set(Double.parseDouble(measurement));
            //gauge.labels(metric).set(Double.parseDouble(measurement));

            push_gateway.pushAdd(registry, jobName, grouping_key);
            //push_gateway.push(registry, jobName, grouping_key);

            logger.info("Successfully Pushed to push gateway.");

        } catch (Exception ex) {
            logger.error("Prometheus push gateway sending failed.", ex);
        } finally {
            registry.clear();
        }
    }
}
