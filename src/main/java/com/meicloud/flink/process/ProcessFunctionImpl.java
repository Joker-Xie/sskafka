package com.meicloud.flink.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.meicloud.flink.shceme.utils.Json2Row;
import com.meicloud.flink.shceme.utils.ParserJson;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Map;

public class ProcessFunctionImpl extends ProcessFunction<String, Row> implements Serializable {
    private Map<String, OutputTag<String>> outputTags;
    private String schema;
    private Json2Row json2row;
    private ParserJson parserJson;

    private static final long serialVersionUID = 6519708208454827535L;

    public ProcessFunctionImpl(Map<String, OutputTag<String>> outputTags, String schema) {
        this.outputTags = outputTags;
        this.schema = schema;
        this.json2row = Json2Row.getInstance();
        this.parserJson = ParserJson.getInstance();
    }

    @Override
    public void processElement(String value, Context ctx, Collector<Row> out) throws Exception {
        if (value != null) {
            if (isJson(value)) {
                if (parserJson.matchSize(value, schema)) {
                    if (parserJson.matchType(value, schema)) {
                        Row row = json2row.addSchema(value, schema);
                        System.out.println("check data is error ==>   "+row.toString());
                        out.collect(row);
                    } else {
                        ctx.output(outputTags.get("side_output_filds_notMatch"), value);
                    }
                } else {
                    ctx.output(outputTags.get("side_output_size_notMatch"), value);
                }
            } else {
                ctx.output(outputTags.get("side_output_notJson"), value);
            }
        }
    }


    private boolean isJson(String value) {
        if (value != null) {
            try {
                JSON.parseObject(value);
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }
}
