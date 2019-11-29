package com.meicloud.flink.asynstream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

public class KafkaAsynStream implements AsyncFunction<String, Row>, ResultTypeQueryable {

    @Override
    public TypeInformation getProducedType() {
        return null;
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<Row> resultFuture) throws Exception {

    }
}
