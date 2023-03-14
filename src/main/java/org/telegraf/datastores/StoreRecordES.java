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
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class StoreRecordES implements Storable {
    private static final Logger logger = LogManager.getLogger(StoreRecordES.class);
    ElasticsearchClient client;
    IndexRequest<JsonData> req;
    private String es_hostname = "";
    private int es_port = 9200;
    private String es_index_retention_days = "3d";

    public StoreRecordES(String host, int port, String retention_days) {
        es_hostname = host;
        es_port = port;
        es_index_retention_days = retention_days;
        this.set_client();
        logger.info("Elasticsearch storage initialized.");
    }

    public void set_client() {
        // Create the low-level client
        RestClient restClient = RestClient.builder(
                new HttpHost(es_hostname, es_port)).build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        client = new ElasticsearchClient(transport);

        // Create the policy for index retention
        //curl -X GET "es-master-headless.observability.svc.cluster.local:9200/_ilm/policy/obs_metrics_policy?pretty"
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPut httpPut = new HttpPut("http://" + es_hostname + ":" + es_port + "/_ilm/policy/obs_metrics_policy?pretty");

        String policyJson = "{" +
                "\"policy\": {" +
                "\"_meta\": {" +
                "   \"description\": \"used for observability metrics\"," +
                "            \"project\": {" +
                "        \"name\": \"Kafka consumer\"," +
                "                \"department\": \"AIDA\"" +
                "    }" +
                "}," +
                "\"phases\": { " +
                "    \"hot\": { " +
                "        \"min_age\": \"0ms\", " +
                "                \"actions\": { " +
                "            \"set_priority\": { " +
                "                \"priority\": 100 " +
                "            }" +
                "        }" +
                "    }," +
                "    \"delete\": { " +
                "        \"min_age\": \""+es_index_retention_days+"\",  " +
                "                \"actions\": { " +
                "            \"delete\": {} " +
                "        }" +
                "    }" +
                "} " +
                "}" +
                "}";

        httpPut.setHeader("Content-Type", "application/json");

        try {
            StringEntity entity = new StringEntity(policyJson);
            httpPut.setEntity(entity);

            try {
                HttpResponse response = httpClient.execute(httpPut);

                if (response.getStatusLine().getStatusCode() != 200) {
                    logger.error("Failed to create policy: " + response.getStatusLine().getStatusCode());
                } else if (response.getStatusLine().getStatusCode() == 200) {
                    logger.info("Successfully created policy");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("Error creating policy entity for index retention: " + e.getMessage());
        }
    }

    public void close_client() {
        //client. close();
        //System.out.println("Close connection.");
    }

    @Override
    public void createRecordFile(String filename) {
        // TODO Auto-generated method stub
    }

    @SuppressWarnings({})
    @Override
    public void store_record(String ES_Index, String metric, Map<String, String> record, List<String> labelKeys, List<String> labelValues, String measurement) {
        try {
            ExistsRequest checkIndex = ExistsRequest.of(b -> b
                    .index(ES_Index));
            BooleanResponse indexExists = client.indices().exists(checkIndex);

            if (!indexExists.value()) {
                Reader settingJson = new StringReader(
                        "{" +
                                "     \"settings\":{" +
                                "         \"index.lifecycle.name\": \"obs_metrics_policy\" " +
                                "     }," +
                                "     \"mappings\":{" +
                                "         \"numeric_detection\": \"true\", " +
                                "         \"date_detection\": \"true\" " +
                                "     }" +
                                "}");
                CreateIndexRequest request = CreateIndexRequest.of(b -> b
                        .index(ES_Index).withJson(settingJson));

                boolean created = client.indices().create(request).acknowledged();
                logger.info("Index " + ES_Index + " is created " + created + ".");
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
            logger.info("Successfully record stored in ES.");
        } catch (Exception e) {
            logger.error("Elasticsearch data save failed.", e);
        }
    }
}
