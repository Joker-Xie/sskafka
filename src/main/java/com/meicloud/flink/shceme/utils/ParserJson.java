package com.meicloud.flink.shceme.utils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * @author xiehy11
 * <p>
 * 2019/11/08
 */
public class ParserJson implements Serializable {

    private static final long serialVersionUID = 569252653027583406L;
    private static ParserJson instance;
    private static Gson gson;

    public static ParserJson getInstance() {
        if (instance == null) {
            instance = new ParserJson();
            gson = new Gson();
        }
        return instance;
    }


    private JsonObject getJsonObjByStr(String str) {
        return gson.fromJson(str, JsonObject.class);
    }

    private JsonArray getJsonArrByStr(String str) {
        return gson.fromJson(str, JsonArray.class);
    }

    private void getJsonObjectValues(String str, List list) {
        JsonObject json = getJsonObjByStr(str);
        Row row = new Row(json.keySet().size());
        int pos = 0;
        for (String key : json.keySet()) {
            String svalue;
            JsonElement jsonElement = json.get(key);
            try {
                svalue = jsonElement.getAsString().trim();
            } catch (Exception e) {
                svalue = jsonElement.toString().trim();
            }
            if (jsonElement.isJsonObject()) {
                getJsonObjectValues(svalue, list);
            } else if (jsonElement.isJsonArray()) {
                getJsonArrayValues(svalue, list);
            } else {
                list.add(key);
            }
            pos++;
        }
    }

    public void getJsonArrayValues(String str, List list) {
        JsonArray json = getJsonArrByStr(str);
        Iterator keySet = json.iterator();
        String schemaKey = null;
        int i = 0;
        while (keySet.hasNext()) {
            String key = keySet.next().toString();
            getJsonObjectValues(key, list);
            i++;
        }
    }

    public boolean matchType(String value, String schema) {
        List vlist = new ArrayList();
        List slist = new ArrayList();
        getJsonObjectValues(value, vlist);
        getJsonObjectValues(schema, slist);
        return vlist.removeAll(slist);
    }

    public boolean matchSize(String value, String schema) {
        List vlist = new ArrayList();
        List slist = new ArrayList();
        getJsonObjectValues(value, vlist);
        getJsonObjectValues(schema, slist);
        return vlist.size() == slist.size();
    }
}
