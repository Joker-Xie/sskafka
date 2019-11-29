package com.meicloud.flink.stream.utils;


import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.meicloud.spark.entity.CaseVo.*;

import java.util.Map;

/**
 * @author xiehy11
 * <p>
 * 2019/11/07
 */
public class KafkaUtils {


    public static void getDataMetaInfo(KafkaStaticConfVo kafkaStaticConfVo) {
        String dataType = kafkaStaticConfVo.dataType();
        switch (dataType) {
            case "json":
                String schema = kafkaStaticConfVo.dataSchema();
                Gson g = new Gson();
                JsonElement jsonTree = g.toJsonTree(schema);
            case "txt":

            default:

        }


    }

    public static void main(String[] args) {
        String schema ="{ \"item_parser_code\": \"string\", \"detail_url\": \"string\", \"shop_name_eng\": \"string\", \"page_model\": \"string\", \"status_id\": \"string\", \"jan_code\": \"string\", \"parser_code\": \"string\", \"item_id\": \"string\", \"tax_price\": \"string\", \"pretax_price\": \"string\", \"shop_name\": \"string\", \"title\": \"string\", \"test\": { \"platform\": \"string\", \"fetch_time\": \"long\", \"store_kafka_topic\": \"string\", \"shop_id\": \"string\", \"sellOutString\": \"string\", \"integral\": \"string\", \"integral_times\": \"string\", \"item_url\": \"string\", \"platform_eng\": \"string\", \"store_redis_key\": \"string\", \"brand\": \"string\", \"model\": \"string\", \"seed_unique\": \"string\" } }";


        Gson g = new Gson();
        JsonObject obj = g.fromJson(schema,JsonObject.class);
        for (Map.Entry<String, JsonElement> set : obj.entrySet()) {//通过遍历获取key和value
            System.out.println(set.getKey() + "_" + set.getValue());
        }

        System.out.println("xxxxx");
    }
}
