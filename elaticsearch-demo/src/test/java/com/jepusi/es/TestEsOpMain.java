package com.jepusi.es;

import com.jepusi.ElaticsearchApplication;
import com.jepusi.entities.Product;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.Date;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestEsOpMain {
    @Autowired
    private ElasticsearchOperations elasticsearchOperations;
    @Test
    public void testCreate() {
        Product product = new Product();
        product.setId(1);
        product.setCreateAt(new Date());
        product.setTitle("3050中段甜品显卡");
        product.setDescription("8G内存性价比高端光追显卡");
        elasticsearchOperations.save(product);
    }
    @Test
    public void testGet() {
        Product product = elasticsearchOperations.get("1", Product.class);
        System.out.println(product);
    }
    @Test
    public void testDelete() {
        Product product = new Product();
        product.setId(1);
        String delete = elasticsearchOperations.delete(product);
        System.out.println(delete);
    }
    @Test
    public void testUpdate() {
        Product product = new Product();
        product.setId(1);
        product.setPrice(2999.99);
        product.setTitle("3060显卡");
        Product save = elasticsearchOperations.save(product);
        System.out.println(save);

    }
    @Test
    public void testQueryAll() {
        SearchHits<Product> search = elasticsearchOperations.search(Query.findAll(), Product.class);
        search.forEach(his ->{
            System.out.println("id: "+his.getId());
            System.out.println("score: "+his.getScore());
            Product content = his.getContent();
            System.out.println("product: "+content);
        });
    }
}
