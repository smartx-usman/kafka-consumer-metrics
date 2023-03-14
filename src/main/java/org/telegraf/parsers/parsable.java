package org.telegraf.parsers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.telegraf.datastores.Storable;

public interface parsable {
    void parse_record(ConsumerRecord<String, String> record, String es_index, Storable data_store_class);
}
