package com.meicloud.flink.udf;


import org.apache.flink.table.functions.ScalarFunction;

public class FlinkScalarFunction extends ScalarFunction {

    private int factor = 12;

    public String eval(String s) {
        return s.hashCode() * factor+"";
    }

}
