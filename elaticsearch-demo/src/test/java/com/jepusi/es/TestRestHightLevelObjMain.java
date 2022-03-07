package com.jepusi.es;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jepusi.entities.Product;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestRestHightLevelObjMain {
    @Resource
    private RestHighLevelClient restHighLevelClient;

    /**
     * 把对象放进es中
     */
    @Test
    public void testRestHightLevelObj() throws IOException {
        Product product = new Product();
        product.setId(8);
        product.setTitle("地平线5");
        product.setPrice(243.5);
        product.setCreateAt(new Date());
        product.setDescription("竞速游戏，地平线");
        IndexRequest indexRequest = new IndexRequest("products");
        indexRequest.id(product.getId().toString())
                .source(new ObjectMapper().writeValueAsString(product), XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(index.status());

    }

    /**
     * 获取es数据，并把其数据序列化到对象中
     * @throws IOException
     */
    @Test
    public void testRestHightLevelObjRead() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("description","游戏"))
                .highlighter(new HighlightBuilder().field("description").requireFieldMatch(false).preTags("<span style='color:red'>").postTags("</span>"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(search.getHits().getTotalHits().value);
        System.out.println(search.getHits().getMaxScore());
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            //System.out.println(hit.getSourceAsString());
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();

            Product product = new ObjectMapper().readValue(hit.getSourceAsString(), Product.class);
            if (highlightFields.containsKey("description")) {
                product.setDescription(highlightFields.get("description").fragments()[0].toString());
            }
            System.out.println(product);
        }
    }
    @Test
    public void testAggsPrice() throws IOException {
        SearchRequest searchRequest = new SearchRequest("fruit");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(AggregationBuilders.terms("group_price").field("price"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = search.getAggregations();
        ParsedDoubleTerms parsedDoubleTerms = aggregations.get("group_price");
        List<? extends Terms.Bucket> buckets = parsedDoubleTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.getKey()+" "+bucket.getDocCount());

        }
    }

    @Test
    public void testAggsTitle() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(AggregationBuilders.terms("group_title").field("title"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = search.getAggregations();
        ParsedStringTerms parsedStringTerms = aggregations.get("group_title");
        List<? extends Terms.Bucket> buckets = parsedStringTerms.getBuckets();
        buckets.forEach(bucket -> {
            System.out.println(bucket.getKey()+" "+bucket.getDocCount());
        });

    }
    @Test
    public void testSum() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(AggregationBuilders.sum("sum_price").field("price"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = search.getAggregations();
        ParsedSum sum_price = aggregations.get("sum_price");
        System.out.println(sum_price.getValue());
    }
    @Test
    public void testAvg() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(AggregationBuilders.avg("avg_price").field("price"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = search.getAggregations();
        ParsedAvg parsedAvg = aggregations.get("avg_price");
        System.out.println(parsedAvg.getValue());
    }
}
