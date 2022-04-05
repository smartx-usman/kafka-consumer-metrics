package org.telegraf.datastores;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
        //System.out.println("Close connection.");
    }

    @SuppressWarnings({})
    @Override
    public void store_record(String ES_Index, Map<String, Object> record) {
        try {
            CreateIndexRequest request = CreateIndexRequest.of(b -> b
                    .index(ES_Index));
            ExistsRequest checkIndex = ExistsRequest.of(b -> b
                    .index(ES_Index));
            BooleanResponse indexExists = client.indices().exists(checkIndex);

            if (!indexExists.value()) {
                boolean created = client.indices().create(request).acknowledged();
                System.out.println("Index " + ES_Index + " is created " + created + ".");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            String jsonRecord = mapper.writeValueAsString(record);

            InputStream stream = new ByteArrayInputStream(jsonRecord.getBytes(StandardCharsets.UTF_8));

            req = IndexRequest.of(b -> b
                            .index(ES_Index)
                            .withJson(stream)
            );

            client.index(req);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
