package com.meicloud.flink.setting;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import scala.Tuple2;

public class FlinkCommonStreamUtils {
    public static Tuple2<StreamExecutionEnvironment,StreamTableEnvironment> getFlinkStreamEnv() {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env, bsSettings);
        return new Tuple2(env,table);
    }
}
