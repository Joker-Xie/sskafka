package com.meicloud.flink.shceme.utils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Iterator;


/**
 * @author xiehy11
 * <p>
 * 2019/11/08
 */
public class Json2Row implements Serializable {

    private static final long serialVersionUID = 569252653027583406L;
    private static Json2Row instance;
    private static Gson gson;

    public static Json2Row getInstance() {
        if (instance == null) {
            instance = new Json2Row();
            gson = new Gson();
        }
        return instance;
    }


    public Row addSchema(String value, String schema) {
        return getJsonObjectValues(value, schema);
    }

    private JsonObject getJsonObjByStr(String str) {
        return gson.fromJson(str, JsonObject.class);
    }

    private JsonArray getJsonArrByStr(String str) {
        return gson.fromJson(str, JsonArray.class);
    }

    private Row getJsonObjectValues(String str, String schema) {
        JsonObject json = getJsonObjByStr(str);
        JsonObject schemaJson = getJsonObjByStr(schema);
        Row row = new Row(json.keySet().size());
        int pos = 0;
        for (String key : json.keySet()) {
            Object jvalue;
            String svalue;
            JsonElement jsonElement = json.get(key);
            JsonElement schemaElement = schemaJson.get(key);
            if (schemaElement != null) {
                try {
                    svalue = schemaElement.getAsString().trim();
                    jvalue = translate(svalue, jsonElement);
                } catch (Exception e) {
                    svalue = schemaElement.toString().trim();
                    jvalue = jsonElement.toString().trim();
                }
                if (jsonElement.isJsonObject()) {
                    row.setField(pos, getJsonObjectValues(jvalue.toString(), svalue));
                } else if (jsonElement.isJsonArray()) {
                    row.setField(pos, getJsonArrayValues(jvalue.toString(), svalue));
                } else {
                    row.setField(pos, jvalue);
                }
                pos++;
            }
        }
        return row;
    }

    private static Object translate(String svalue, JsonElement jsonElement) {
        switch (svalue) {
            case "string":
            case "timestamp":
            case "date":
                return jsonElement.getAsString();
            case "int":
                return jsonElement.getAsInt();
            case "float":
                return jsonElement.getAsFloat();
            case "double":
                return jsonElement.getAsDouble();
            case "boolean":
                return jsonElement.getAsBoolean();
            case "long":
                return jsonElement.getAsLong();
            case "byte":
                return jsonElement.getAsByte();
            default:
                break;

        }
        return null;
    }

    public Row[] getJsonArrayValues(String str, String schema) {
        JsonArray json = getJsonArrByStr(str);
        JsonArray schemaJson = getJsonArrByStr(schema);
        Iterator keySet = json.iterator();
        Iterator schemaKeySet = schemaJson.iterator();
        String schemaKey = null;
        Row[] row = new Row[(json.size())];
        int i = 0;
        while (keySet.hasNext()) {
            String key = keySet.next().toString();
            if (schemaKeySet.hasNext()) {
                schemaKey = schemaKeySet.next().toString();
            }
            row[i] = getJsonObjectValues(key, schemaKey);
            i++;
        }
        return row;
    }

    public static void main(String[] args) {
        Json2Row json2row = Json2Row.getInstance();
        String schema = "{\"word\": \"string\",\"frequency\": [{\"test\": \"string\",\"word\": \"string\"}],\"count\": \"int\"}";
        String json = "{\"word\": \"nihao\",\"frequency\": [{\"test\": \"bb\",\"word\": \"kk\"},{\"test\": \"bb1\",\"word\": \"kk1\"}],\"count\":6}";
        Row row = json2row.addSchema(json, schema);
        System.out.println("xxxx");
    }
}
