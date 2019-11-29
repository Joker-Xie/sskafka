package com.meicloud.flink.table.sink;


import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class RetrackKafkaSinkFunction implements SinkFunction<Row> {
  /*  private String brokerList;
    private String topic;
    private String groupId;*/

    /*public RetrackKafkaSinkFunction(Builder builder) {
        this.brokerList = builder.brokerList;
        this.groupId = builder.groupId;
        this.topic = builder.topic;
    }*/

    public void sendMsgByKafka(String msg) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.112.128:9092");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(props);
        if (msg != null) {
            producer.send(new ProducerRecord("output", msg), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    @Override
    public void invoke(Row value) throws Exception {
        if (value != null) {
            String str = value.toString();
            sendMsgByKafka(str);
        }
    }

  /*  public static class Builder {
        private String brokerList;
        private String topic;
        private String groupId;

        public Builder setBrokerList(String brokerList) {
            this.brokerList = brokerList;
            return this;
        }

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder setGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public RetrackKafkaSinkFunction builder() {
            return new RetrackKafkaSinkFunction(this);
        }
    }*/
}
