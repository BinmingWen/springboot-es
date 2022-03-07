package com.jepusi.es;

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
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestRestHighLevelClientMain {
    @Resource
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void testCreateIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("products");
        createIndexRequest.mapping("{\n" +
                "    \"properties\":{\n" +
                "      \"id\":{\n" +
                "        \"type\":\"integer\"\n" +
                "      },\n" +
                "      \"title\":{\n" +
                "        \"type\":\"text\",\n" +
                "        \"analyzer\": \"ik_max_word\"\n" +
                "      },\n" +
                "      \"price\":{\n" +
                "        \"type\":\"double\"\n" +
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
        IndexRequest createIndexRequest = new IndexRequest("products");
        createIndexRequest.source("{\n" +
                "  \"id\":2,\n" +
                "  \"title\":\"2077赛博朋克\",\n" +
                "  \"price\":\"127\",\n" +
                "  \"create_at\":\"2022-12-22\",\n" +
                "  \"description\":\"未来科技游戏，赛博朋克风格\"\n" +
                "}",XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(index.status());
        System.out.println(index.toString());
    }

    @Test
    public void testUpdate() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("products","TDglWH8BmWcGSmiNpQpw");
        updateRequest.doc("{\"price\":\"299\"}",XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update.getResult());
        System.out.println(update.status());

    }
    @Test
    public void testGet() throws IOException {
        GetRequest getRequest = new GetRequest("products", "1");
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());

    }
    @Test
    public void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        hits.forEach(hit ->{
            System.out.println(hit.getSourceAsString());
        });
    }

    @Test
    public void testSearchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0).size(4).sort("price", SortOrder.DESC)  //参数一：根据那个字段排序，参数二：字段排序结果
                .query(QueryBuilders.termQuery("description","游戏"))
                //.fetchSource(new String[]{"title"},new String[]{}) 参数1；包含参数，参数2：排除参数结果
                .highlighter(new HighlightBuilder().field("description").field("title").requireFieldMatch(false).postTags("</span>").preTags("<span style='color:red'"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        System.out.println("总数为："+search.getHits().getTotalHits().value);
        hits.forEach(hit ->{
            System.out.println(hit.getSourceAsString());
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (highlightFields.containsKey("title")) {
                System.out.println("title高亮结果："+highlightFields.get("title").fragments()[0]);
            }
            if (highlightFields.containsKey("description")) {
                System.out.println("description高亮结果："+highlightFields.get("description").fragments()[0]);
            }

        });

    }

    @Test
    public void testFilterSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery())
                .postFilter(QueryBuilders.rangeQuery("price").gt("50").lt("200"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总数为："+search.getHits().getTotalHits().value);
        SearchHits hits = search.getHits();
        hits.forEach(hit ->{
            System.out.println(hit.getSourceAsString());
        });
    }
}
