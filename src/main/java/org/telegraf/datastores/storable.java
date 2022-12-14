package org.telegraf.datastores;

import java.io.IOException;
import java.util.Map;

public interface storable {
    void store_record(String ES_Index, Map<String, String> record) throws IOException;
}
