package com.meicloud.flink.shceme.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * 继承Types类,重写ROW_NAMED方法
 * 参数实现数值写入
 */
public class TypeInfomationDecorate extends Types {

    public static TypeInformation<Row> ROW_NAMED(String[] fieldNames, TypeInformation[] types) {
        return new RowTypeInfo(types, fieldNames);
    }
}
