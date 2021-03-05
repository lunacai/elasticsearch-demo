package com.example.stoelasticsearchdemo;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Component
public class EsService {
    private static final String TYPE = "type";
    public static RestHighLevelClient client;

    public RestHighLevelClient getClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("{IP}", 9200, "http")));
        return client;
    }

    public boolean createIndex(String index) {
        CreateIndexRequest request = new CreateIndexRequest(index);
        request.source("{\n" +
                "    \"settings\" : {\n" +
                "        \"number_of_shards\" : 1,\n" +
                "        \"number_of_replicas\" : 0\n" +
                "    },\n" +
                "    \"mappings\" : {\n" +
                "    },\n" +
                "    \"aliases\" : {\n" +
                "        \"twitter_alias\" : {}\n" +
                "    }\n" +
                "}", XContentType.JSON);
        CreateIndexResponse createIndexResponse = null;
        try {
            client = getClient();
            createIndexResponse = client.indices().create(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return createIndexResponse.isAcknowledged();
    }

    //判断索引是否存在
    public boolean isIndexExists(String index) {
        GetIndexRequest request = new GetIndexRequest();
        request.indices(index);
        request.local(false);
        request.humanReadable(true);
        boolean exists = false;
        try {
            client = getClient();
            exists = client.indices().exists(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exists;
    }

    public boolean deleteIndex(String index) {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        DeleteIndexResponse deleteIndexResponse = null;
        try {
            client = getClient();
            deleteIndexResponse = client.indices().delete(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return deleteIndexResponse.isAcknowledged();
    }

    public String getById(String index, String id) {
        try {
            GetRequest request = new GetRequest(index, TYPE, id);
            client = getClient();
            GetResponse getReponse = client.get(request);
            if (getReponse.isExists()) {
                String result = getReponse.getSourceAsString();
                System.out.println("result>>>>>>>>>>>>" + result);
                return result;
            }
        } catch (Exception e) {
            throw new RuntimeException("getById exception", e);
        }
        return null;
    }

    public <T> String insertIndex(String index, String id, T data) {
        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(data);
        IndexRequest indexRequest = new IndexRequest(index, TYPE, jsonObject.getString(id))
                .source(jsonObject.toJSONString(), XContentType.JSON);
        IndexResponse indexResponse = null;
        try {
            client = getClient();
            indexResponse = client.index(indexRequest);
        } catch (IOException e) {
            throw new RuntimeException("es插入异常", e);
        }
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            return indexResponse.getId();
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            return indexResponse.getId();
        }
        return null;
    }


    public String putIndex(String index, String id, Map<String, Object> settings) {
        IndexRequest indexRequest = new IndexRequest(index, TYPE, id)
                .source(JSONObject.toJSON(settings).toString(), XContentType.JSON);
        UpdateRequest updateRequest = new UpdateRequest(index, TYPE, id)
                .doc(JSONObject.toJSON(settings).toString(), XContentType.JSON)
                .upsert(indexRequest);
        try {
            client = getClient();
            UpdateResponse updateResponse = client.update(updateRequest);
            return updateResponse.toString();
        } catch (IOException e) {
            throw new RuntimeException("索引修改setting异常", e);
        }
    }

    public String delIndex(String index, String id) {
        DeleteRequest request = new DeleteRequest(index, TYPE, id);
        try {
            client = getClient();
            DeleteResponse deleteResponse = client.delete(request);
            return deleteResponse.toString();
        } catch (IOException e) {
            throw new RuntimeException("索引修改setting异常", e);
        }
    }
}
