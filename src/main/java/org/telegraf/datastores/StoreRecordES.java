package org.telegraf.datastores;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.ExistsRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.ObjectBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.json.simple.JSONObject;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

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
            GetIndexRequest getIndex = GetIndexRequest.of(b -> b
                    .index(ES_Index));



            //GetIndexResponse indexResponse = client.indices().get(getIndex);

            ExistsRequest checkIndex = ExistsRequest.of(b -> b
                    .index(ES_Index));
            BooleanResponse indexExists = client.indices().exists((Function<co.elastic.clients.elasticsearch.indices.ExistsRequest.Builder, ObjectBuilder<co.elastic.clients.elasticsearch.indices.ExistsRequest>>) checkIndex);

            System.out.println(indexExists);

            boolean exists=false;
            if (indexExists.value())
            {
                boolean created = client.indices().create(request).acknowledged();
                System.out.println("Index is created.");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            String jsonRecord = new ObjectMapper().writeValueAsString(record);

            FileReader file = new FileReader(jsonRecord);

            req = IndexRequest.of(b -> b
                    .index(ES_Index)
                    .withJson(file)
            );

            client.index(req);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
