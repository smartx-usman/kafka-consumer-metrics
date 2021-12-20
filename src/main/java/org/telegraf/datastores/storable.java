package org.telegraf.datastores;

import java.util.Map;

public interface storable {
    void store_record(String ES_Index, Map<String, Object> record);
}
