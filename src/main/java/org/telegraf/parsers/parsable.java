package org.telegraf.parsers;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface parsable {
    void parse_record(ConsumerRecord<String, String> record, String es_index);
}
