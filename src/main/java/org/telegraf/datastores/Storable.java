package org.telegraf.datastores;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Storable {
    void createRecordFile(String filename);
    void store_record(String plugin, String metric, Map<String, String> record, List<String> labelKeys, List<String> labelValues, String measurement) throws IOException;
}
