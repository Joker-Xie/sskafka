package com.meicloud.flink.table.source.descriptor;

import com.meicloud.spark.entity.CaseVo.*;

import java.io.Serializable;

public class TableSoueceDescriptor implements Serializable {
    private static final long serialVersionUID = 207896888752807971L;
    private String jsonSchema;
    private SourceDescriptor sourceDescriptor;

    public TableSoueceDescriptor(InputKafkaConfigVo inputConfigVo) {
        this.jsonSchema = inputConfigVo.dataSchema();
        this.sourceDescriptor = new SourceDescriptor();
        this.sourceDescriptor = registerSourceDes(inputConfigVo);
    }

    public String getJsonSchema() {
        return jsonSchema;
    }

    public void setJsonSchema(String jsonSchema) {
        this.jsonSchema = jsonSchema;
    }

    public SourceDescriptor getSourceDescriptor() {
        return sourceDescriptor;
    }

    public void setSourceDescriptor(SourceDescriptor sourceDescriptor) {
        this.sourceDescriptor = sourceDescriptor;
    }

    private SourceDescriptor registerSourceDes(InputKafkaConfigVo inputConfigVo) {
        SourceDescriptor descriptor = sourceDescriptor.setDataType(inputConfigVo.dataType())
                .setSourceType(inputConfigVo.sourceType())
                .setProperties("bootstrap.servers", inputConfigVo.kafkaBootstrapServers())
                .setProperties("topic", inputConfigVo.subscribeContent());
        return descriptor;
    }
}
