package com.meicloud.flink.shceme.utils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.Iterator;


/**
 * @author xiehy11
 * <p>
 * 2019/11/08
 */
public class JsonType2TypeInfomation {


    public TypeInformation[] JsonType2TypeInfos(String str) {
        Gson gson = new Gson();
        JsonObject json = gson.fromJson(str, JsonObject.class);

        Iterator keySet = json.keySet().iterator();
        int size = json.size();
        TypeInformation[] filedTypes = new TypeInformation[size];
        int i = 0;
        while (keySet.hasNext()) {
            String key = keySet.next().toString();
            String value = null;
            try {
                value = json.get(key).getAsString().trim();
            } catch (Exception e) {
                value = json.get(key).toString().trim();
            }
            filedTypes[i] = transfromType(value);
            i++;
        }
        return filedTypes;
    }

    /**
     * 用于将object类型json解析为Row类型.
     *
     * @param str
     */
    public TypeInformation parserJsonObject(String str) {
        Gson gson = new Gson();
        JsonObject json = gson.fromJson(str, JsonObject.class);

        Iterator keySet = json.keySet().iterator();
        int size = json.size();
        String[] fileds = new String[size];
        TypeInformation[] filedTypes = new TypeInformation[size];
        int i = 0;
        while (keySet.hasNext()) {
            String key = keySet.next().toString();
            String value = null;
            try {
                value = json.get(key).getAsString().trim();
            } catch (Exception e) {
                value = json.get(key).toString().trim();
            }
            fileds[i] = key;
            filedTypes[i] = transfromType(value);
            i++;
        }
        return new RowTypeInfo(filedTypes, fileds);
    }


    /**
     * 用于将Array类型json解析成为ObjectArrayTypeInfo类型.
     *
     * @param str
     */
    public TypeInformation parserJsonArray(String str) {
        Gson gson = new Gson();
        JsonArray json = gson.fromJson(str, JsonArray.class);
        Iterator keySet = json.iterator();
        String key = keySet.next().toString();
        RowTypeInfo row = (RowTypeInfo) parserJsonObject(key);
        return ObjectArrayTypeInfo.getInfoFor(Row[].class, row);
    }

    /**
     * 类型转换方法,将json定义的数据类型装换成Types对应类型.
     *
     * @param clazzName
     */
    public TypeInformation<?> transfromType(String clazzName) {
        clazzName = clazzName.toUpperCase();
        switch (clazzName) {
            case "STRING":
                return Types.STRING;
            case "INT":
                return Types.INT;
            case "LONG":
                return Types.LONG;
            case "FLOAT":
                return Types.FLOAT;
            case "DOUBLE":
                return Types.DOUBLE;
            case "TIMESTAMP":
                return Types.SQL_TIMESTAMP;
            case "BYTE":
                return Types.BYTE;
            case "BOOLEAN":
                return Types.BOOLEAN;
            default:
                if (clazzName.startsWith("{")) {
                    return parserJsonObject(clazzName);
                } else if (clazzName.startsWith("[")) {
                    return parserJsonArray(clazzName);
                }
                break;
        }
        return null;
    }


    /**
     * 获取json字符串最外层key值
     *
     * @param str
     */
    public static String[] getJsonOuterlayerKey(String str) {
        Gson gson = new Gson();
        JsonObject json = gson.fromJson(str, JsonObject.class);
        int size = json.size();
        String[] fileds = new String[size];
        json.keySet().toArray(fileds);
        return fileds;
    }



    /**
     * 将Schema转换成RowTypeInfo,
     * 实现根据结果自动注册结果集类型
     *
     * @param tableSchema
     */
    public static RowTypeInfo schema2TypeInfomation(TableSchema tableSchema) {
        DataType[] dataTypes = tableSchema.getFieldDataTypes();
        TypeInformation[] typeInformations = dataType2TypesInfomation(dataTypes);
        String[] fieldNames = tableSchema.getFieldNames();
        return new RowTypeInfo(typeInformations, fieldNames);
    }

    private static TypeInformation[] dataType2TypesInfomation(DataType[] dataTypes) {
        int size = dataTypes.length;
        TypeInformation[] typeInformations = new TypeInformation[size];
        int i = 0;
        for (DataType dataType : dataTypes) {
            typeInformations[i] = dataTypeTransfromTypeInfomation(dataType.toString());
            i++;
        }
        return typeInformations;
    }

    /**
     * DataType转换成TypeInfomation
     *
     * @param dataType
     */
    private static TypeInformation dataTypeTransfromTypeInfomation(String dataType) {
        dataType = dataType.split(" ")[0];
        switch (dataType) {
            case "STRING":
                return Types.STRING;
            case "INT":
                return Types.INT;
            case "FLOAT":
                return Types.FLOAT;
            case "LONG":
            case "BIGINT":
                return Types.LONG;
            case "TIMESTAMP":
                return Types.SQL_TIMESTAMP;
            case "BOOLEAN":
                return Types.BOOLEAN;
            case "DOUBLE":
                return Types.DOUBLE;
            case "VOID":
                return Types.VOID;
            case "BYTE":
                return Types.BYTE;
        }
        return null;
    }

    public static void main(String[] args) {
        String jsonStr = "{\"item_parser_code\": \"string\",\"detail_url\": \"string\",\"shop_name_eng\": \"string\",\"page_model\": \"string\",\"status_id\": \"string\",\"jan_code\": \"string\",\"parser_code\": \"string\",\"item_id\": \"string\",\"tax_price\": \"string\",\"shop_name\": \"string\",\"title\": [{\"test\": \"string\",\"platform\": \"string\",\"fetch_time\": \"long\",\"store_kafka_topic\": \"string\",\"shop_id\": \"string\",\"sellOutString\": \"string\",\"integral\": \"string\",\"integral_times\": \"string\",\"item_url\": \"string\"}],\"platform_eng\": \"string\",\"store_redis_key\": \"string\",\"model\": \"string\",\"seed_unique\": \"string\"}";
        TypeInformation[] row = new JsonType2TypeInfomation().JsonType2TypeInfos(jsonStr);
        System.out.println("xxxx");
    }
}
