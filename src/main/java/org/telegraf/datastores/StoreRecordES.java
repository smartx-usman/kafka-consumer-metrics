package org.telegraf.datastores;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class StoreRecordES implements storable {
    ElasticsearchClient client;
    IndexRequest<JsonData> req;

    public StoreRecordES() {
        this.set_client();
    }

    public void set_client() {
        // Create the low-level client
        RestClient restClient = RestClient.builder(
                new HttpHost("es-master-0.es-master-headless.monitoring.svc.cluster.local", 9200)).build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        client = new ElasticsearchClient(transport);
    }

    public void close_client() {
        //client. close();
        System.out.println("Close connection.");
    }

    @SuppressWarnings({})
    @Override
    public void store_record(String ES_Index, Map<String, Object> record) {
        CreateIndexRequest request = CreateIndexRequest.of(b -> b
                .index(ES_Index));

        try {
            boolean created = Boolean.TRUE.equals(client.indices().create(request).acknowledged());
            System.out.println("Index is created");
        } catch (IOException e) {
            e.printStackTrace();
        }

        req = IndexRequest.of(b -> b
                .index(ES_Index)
                .withJson((InputStream) record)
        );

        try {
            client.index(req);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
