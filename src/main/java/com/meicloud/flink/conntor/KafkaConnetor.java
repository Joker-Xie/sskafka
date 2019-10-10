package com.meicloud.flink.conntor;

import com.meicloud.flink.JavaConstantUtils;
import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.table.descriptors.Kafka;

public class KafkaConnetor {

    public static Kafka getConnetor(InputKafkaConfigVo inputKafkaConfigVo) {
        Kafka kafka = new Kafka()
                .version(JavaConstantUtils.VERSION)
                .topic(inputKafkaConfigVo.subscribeContent())
                .property(JavaConstantUtils.KAFKA_BOOTSTRAP_SERVER, inputKafkaConfigVo.kafkaBootstrapServers())
                .property(JavaConstantUtils.GROUP_ID, "flink1")
                .property("enable.auto.commit", "false")
                .startFromEarliest();
        return kafka;
    }
}
