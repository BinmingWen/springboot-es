package com.jepusi.es;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * @class: com.jepusi.demo.ElaticsearchRestHighLevelClient
 * @description:
 * @author: 温明彬
 * @company: 广州博瑞信息技术股份有限公司
 * @create: 2022/3/1 14:41
 */

@SpringBootTest
public class ElaticsearchRestHighLevelClient {
    @Resource
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void testCreateIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("products");
        createIndexRequest.mapping("{\n" +
                "    \"properties\": {\n" +
                "      \"id\":{\n" +
                "        \"type\": \"integer\"\n" +
                "      },\n" +
                "      \"price\":{\n" +
                "        \"type\":\"double\"\n" +
                "      },\n" +
                "      \"title\":{\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"create_at\":{\n" +
                "        \"type\":\"date\"\n" +
                "      },\n" +
                "      \"description\":{\n" +
                "        \"type\":\"text\",\n" +
                "        \"analyzer\": \"ik_max_word\"\n" +
                "      }\n" +
                "    }\n" +
                "  }", XContentType.JSON);
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.isAcknowledged());
        restHighLevelClient.close();

    }
    @Test
    public void testIndex() throws IOException {
        IndexRequest indexRequest = new IndexRequest("products");
        indexRequest.id("2");
        indexRequest.source("{\n" +
                "  \"id\":3,\n" +
                "  \"title\":\"坚果R2\",\n" +
                "  \"price\":1999.9,\n" +
                "  \"create_at\":\"2022-03-01\",\n" +
                "  \"description\":\"锥子手机，TNT双系统手机，高级商务手机\"\n" +
                "}",XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(index.status());
    }

    @Test
    public void testGet() throws IOException {
        GetRequest getRequest = new GetRequest("products","2");
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
    }
    @Test
    public void testDelete() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("products","2");
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }

    @Test
    public void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        hits.forEach(o->{
            System.out.println(o.getSourceAsString());
        });

    }

    @Test
    public void testUpdate() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("products","1");
        updateRequest.doc("{\n" +
                "    \"title\":\"坚果pro3\"\n" +
                "  }",XContentType.JSON);
        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
    }

    @Test
    public void testSearchs() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            String sourceAsString = hit.getSourceAsString();
            System.out.println("id " + id+" source"+sourceAsString);
        }

    }
    @Test
    public void testQuery() throws IOException {
        //1.term关键字
        //query(QueryBuilders.termQuery("description","锥子"));
        //2.range查询
        //query(QueryBuilders.rangeQuery("price").gt(1000).lt(5000));
        //3.prefix
        //query(QueryBuilders.prefixQuery("title","坚果"));
        //4.wildcard
        //query(QueryBuilders.wildcardQuery("title","坚果*"));
        query(QueryBuilders.multiMatchQuery("高级","title","description"));

    }

    public void query(QueryBuilder queryBuilder) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        long value = search.getHits().getTotalHits().value;
        System.out.println("总数为："+value);
        float score = search.getHits().getMaxScore();
        System.out.println("总分数为："+score);
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println("id: "+hit.getId()+" source"+hit.getSourceAsString());
        }
    }
}
