package com.meicloud.flink.table.sink;

import com.meicloud.flink.shceme.utils.JsonType2TypeInfomation;
import com.meicloud.flink.shceme.utils.SchemaUtils;
import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Properties;

public class SinkUtils {
    //输出kafka
    public static void load2KafkaSink(Table table, OutputKafkaConfigVo outputKafkaConfigVo, StreamTableEnvironment evTable) {
       /* Schema schema = new Schema().schema(table.getSchema());
        //注册Sink
//        sinkBuildAndRegister(outputKafkaConfigVo)
        Properties kafkaPros = new Properties();
        kafkaPros.setProperty("bootstrap.servers", "192.168.112.128:9092,192.168.112.129:9092");
        evTable.connect(new Kafka().version("universal")
                .topic("output")
                .properties(kafkaPros))
                .withFormat(
                        new Json().deriveSchema()
                )
                .withSchema(schema)
                .inAppendMode()
                .registerTableSink("dstTable");

        DataStream<Row> dataStream = evTable.toRetractStream(table, JsonType2TypeInfomation.schema2TypeInfomation(table.getSchema())).filter(new FilterFunction<Tuple2<Boolean, Row>>() {
            @Override
            public boolean filter(Tuple2<Boolean, Row> value) throws Exception {
                return value.f0;
            }
        }).map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
            @Override
            public Row map(Tuple2<Boolean, Row> value) throws Exception {
                return value.f1;
            }
        }).returns(JsonType2TypeInfomation.schema2TypeInfomation(table.getSchema()));
        evTable.fromDataStream(dataStream).insertInto("dstTable");*/
        TableSchema tableSchema = table.getSchema();
        RowTypeInfo rowType = JsonType2TypeInfomation.schema2TypeInfomation(tableSchema);
        evTable.toRetractStream(table, rowType).print();
    }

    //输出jdbc
    public static void load2JdbcSink(OutputJdbcConfigVo outputJdbcConfigVo, StreamTableEnvironment evTable) {
       /* TableSchema tableSchema = table.getSchema();
        RowTypeInfo rowType = JsonType2TypeInfomation.schema2TypeInfomation(tableSchema);
        evTable.toAppendStream(table, rowType).print();*/
    }

    //输出ES
    public static void load2EsSink(OutputEsConfigVo outputEsConfigVo, StreamTableEnvironment evTable) {

    }

    //输出File
    public static void load2FileSink(OutputFileConfigVo outputFileConfigVo, StreamTableEnvironment evTable) {

    }

    //输出Hbase
}
