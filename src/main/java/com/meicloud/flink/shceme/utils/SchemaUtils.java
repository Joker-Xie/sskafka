package com.meicloud.flink.shceme.utils;

import com.alibaba.fastjson.JSONObject;
import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Schema;

public class SchemaUtils {


    public static Schema getDataSchema(String json) {
        Schema elasticSchema = parserSchema(new Schema(), json);
        return elasticSchema;
    }

    private static Schema parserSchema(Schema elasticSchema, String json) {
        JsonType2TypeInfomation jsonUtils = new JsonType2TypeInfomation();
        TypeInformation[] typeInformation = jsonUtils.JsonType2TypeInfos(json);
        String[] fileds = JsonType2TypeInfomation.getJsonOuterlayerKey(json);
        TableSchema tableSchema = new TableSchema(fileds, typeInformation);
        Schema schema = elasticSchema.schema(tableSchema);
        return schema;
    }

    public static TableSchema json2TableSchema(String json) {
        JsonType2TypeInfomation jsonUtils = new JsonType2TypeInfomation();
        TypeInformation[] typeInformation = jsonUtils.JsonType2TypeInfos(json);
        String[] fileds = JsonType2TypeInfomation.getJsonOuterlayerKey(json);
        TableSchema tableSchema = new TableSchema(fileds, typeInformation);
        return tableSchema;
    }

}
