package com.meicloud.saprk.hdfs.utils;


import com.meicloud.spark.utils.PropertiesScalaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import static com.meicloud.saprk.hdfs.utils.JavaStringUtils.ts2yyyyMMdd;

public class HDFSRawConsumer {
    private final KafkaConsumer consumer;
    private final Collection topic = new ArrayList();
    private final HDFSWriter writer = new HDFSWriter();

    public HDFSRawConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", PropertiesScalaUtils.getString("kafka_broker"));
        props.put("group.id", "hive");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(props);
    }

    public void watchErrorTopic(String targetTopic) {
        ((ArrayList) topic).add(targetTopic + "_error_data");
        consumer.subscribe(topic);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            Iterator recordItor = records.iterator();
            while (recordItor.hasNext()) {
                ConsumerRecord<String, String> record = (ConsumerRecord) recordItor.next();
                String msg = record.value();
                Long yyyyMMDD = System.currentTimeMillis();
                String path = PropertiesScalaUtils.getString("error_msg_hive_table_addr") + "/data_part_" + ts2yyyyMMdd(yyyyMMDD);
                if (null != msg) {
                    writer.writeLog2HDFS(path, msg);
                }
            }
        }
    }

    public static void main(String[] args) {
        HDFSRawConsumer consumer = new HDFSRawConsumer();
        consumer.watchErrorTopic("gxt.comment.white.redo");


//        Long ts = System.currentTimeMillis();
//        String date = ts2yyyyMMdd(ts);
//        System.out.println();
//        new Timer().schedule(new CloseMyFSDataStream(), 0, 30000);
//        new HDFSRawConsumer().watchErrorTopic("");

    }
}
