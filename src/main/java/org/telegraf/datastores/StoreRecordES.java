package org.telegraf.datastores;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

public class StoreRecordES implements storable {
    RestHighLevelClient client;

    public StoreRecordES() {
        this.set_client();
    }

    public void set_client() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("es-master-0.es-master-headless.monitoring.svc.cluster.local", 9200, "http")));
    }

    public void close_client() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings({})
    @Override
    public void store_record(String ES_Index, Map<String, Object> record) {
        IndexRequest request = new IndexRequest(ES_Index);
        request.source(record, XContentType.JSON);

        IndexResponse response = null;
        try {
            response = client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String index = response.getIndex();
        long version = response.getVersion();
    }
}
