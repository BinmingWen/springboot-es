package com.jepusi.entities;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Date;

//@Document(indexName = "products",createIndex = true)
@Data
public class Product {
    //@Id
    private Integer id;
    //@Field(type = FieldType.Keyword)
    private String title;
   // @Field(type = FieldType.Double)
    private Double price;
    //@Field(type = FieldType.Date,format = DateFormat.basic_date_time)
    @JsonFormat(pattern = "yyyy-MM-dd")
    @JsonProperty("create_at")
    private Date createAt;
   // @Field(type = FieldType.Text,analyzer = "ik_max_word")
    private String description;
}
