package com.meicloud.flink.shceme.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Set;

public class SchemaUtils {


    public static TypeInformation<Row> getJsonSchema(InputKafkaConfigVo inputKafkaConfigVo) {
        String schema = inputKafkaConfigVo.dataSchema();
        JSONObject obj = JSON.parseObject(schema);
        Set<String> keySet = obj.keySet();
        String[] keyArr = (String[]) keySet.toArray();
        TypeInformation[] typeArr = new TypeInformation[keySet.size()];
        int i = 0;
        for (String key : keyArr) {
            String strType = obj.getString(key);
            typeArr[i] = jsonType2RowType(strType);
            i++;
        }
        return TypeInfomationDecorate.ROW_NAMED(keyArr, typeArr);
    }

    public static Schema getSchema(InputKafkaConfigVo inputKafkaConfigVo) {
        return null;
    }

    private static TypeInformation jsonType2RowType(String strType) {
        if (strType != null && strType != "") {
            switch (strType.toLowerCase()) {
                case "string":
                    return Types.STRING;
                case "int":
                    return Types.INT;
                case "long":
                    return Types.LONG;
                case "date":
                    return Types.SQL_DATE;
                case "timestamp":
                    return Types.SQL_TIMESTAMP;
                case "float":
                    return Types.FLOAT;
                case "short":
                    return Types.SHORT;
                case "byte":
                    return Types.BYTE;
                case "double":
                    return Types.DOUBLE;
                case "boolean":
                    return Types.BOOLEAN;
                default:
                    return Types.VOID;
            }
        }
        return Types.VOID;
    }

}
