package com.meicloud.flink.table.source;

import com.meicloud.flink.process.ProcessFunctionImpl;
import com.meicloud.flink.shceme.utils.JsonType2TypeInfomation;
import com.meicloud.flink.shceme.utils.SchemaUtils;
import com.meicloud.flink.table.source.descriptor.SourceDescriptor;
import com.meicloud.flink.table.source.descriptor.TableSoueceDescriptor;
import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LocalKafkaTableSource implements StreamTableSource<Row>, Serializable, ResultTypeQueryable<Row> {

    private static final long serialVersionUID = -6866945155741952559L;
    private TableSoueceDescriptor tableSoueceDescriptor;
    private transient TableSchema schema;
    private String jsonSchema;
    private SourceDescriptor descriptor;

    public LocalKafkaTableSource(InputKafkaConfigVo inputConfigVo) {
        this.tableSoueceDescriptor = new TableSoueceDescriptor(inputConfigVo);
        this.descriptor = tableSoueceDescriptor.getSourceDescriptor();
        this.jsonSchema = tableSoueceDescriptor.getJsonSchema();
        this.schema = SchemaUtils.json2TableSchema(jsonSchema);
    }


    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        FlinkKafkaConsumer consumer = createConsumerByConf(descriptor.getConf());
        DataStream srcDataStream = execEnv.addSource(consumer);
        return addErrorProcess(srcDataStream).returns(JsonType2TypeInfomation.schema2TypeInfomation(schema));
    }

    private <T> SingleOutputStreamOperator addErrorProcess(DataStream srcDataStream) {
        Map<String, OutputTag<String>> outputTagMap = createErrorOutputTags();
        SingleOutputStreamOperator conplexDateStream = srcDataStream.process(new ProcessFunctionImpl(outputTagMap, jsonSchema));
        return conplexDateStream.returns(JsonType2TypeInfomation.schema2TypeInfomation(schema));
    }

    /**
     * 创建异常通道筛选器
     *
     * @param
     */
    private Map<String, OutputTag<String>> createErrorOutputTags() {
        Map<String, OutputTag<String>> tagMap = new HashMap();
        for (int i = 0; i < 3; i++) {
            switch (i) {
                case 0:
                    tagMap.put("side_output_notJson", new OutputTag<String>("side-output-nojson"){});
                    break;
                case 1:
                    tagMap.put("side_output_size_notMatch", new OutputTag<String>("side-output-size-notMatch"){});
                    break;
                case 2:
                    tagMap.put("side_output_filds_notMatch", new OutputTag<String>("side-output-filds-notMatch"){});
                    break;
                default:
                    break;
            }
        }
        return tagMap;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public DataType getProducedDataType() {
        return schema.toRowDataType();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return JsonType2TypeInfomation.schema2TypeInfomation(getTableSchema());
    }


    private FlinkKafkaConsumer createConsumerByConf(Map<String, Object> conf) {
        Properties properties = (Properties) conf.get("general_props");
        String topic = properties.getProperty("topic");
        String starupMode = null;
        /*try {
            starupMode = conf.get("startup_mode").toString();
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        Properties attachProps = new Properties();
        attachProps.put("bootstrap.servers", properties.get("bootstrap.servers"));
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), attachProps);
        if (starupMode == null) {
            consumer.setStartFromEarliest();
        } else {
            //TODO
        }
        return consumer;
    }
}
