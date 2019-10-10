package com.meicloud.flink;


import java.util.HashMap;
import java.util.Map;

public class JavaConstantUtils {

    //流处理日志级别
    public static final String LOG_LEVEL_ERROR = "ERROR";
    public static final String LOG_LEVEL_WORN = "WORN";
    public static final String LOG_LEVEL_INFO = "INFO";

    //输入源 --KAFKA --参数>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    public static final String VERSION = "universal";
    public static final String GROUP_ID = "group.id";
    public static final String KAFKA_BOOTSTRAP_SERVER = "kafka.bootstrap.servers";
    public static final String STARTING_OFFSETS = "startingOffsets";
    public static final String ENDING_OFFSET = "endingOffsets";
    public static final String MAX_OFFSETS_PER_TRIGGER = "maxOffsetsPerTrigger";


    //输出源 --ES --参数>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    public static final String ES_NET_HTTP_AUTH_USER = "es.net.http.auth.user";
    public static final String ES_NET_HTTP_AUTH_PASS = "es.net.http.auth.pass";
    public static final String ES_NODES = "es.nodes";
    public static final String ES_PORT = "es.port";
    public static final String ES_RESOURCE = "es.resource";
    public static final String ES_NODE_WAN_ONLY = "es.nodes.wan.only";
    public static final String PUSH_DOWN = "pushdown";
    public static final String ES_MAPPING_ID = "es.mapping.id";
    public static final String ORG_ELASTICSEARCH_SPARK_SQL = "org.elasticsearch.spark.sql";

    //输出源  --Kafka--参数
    public static final String KAFKA_TOPIC = "topic";
    //流处理 hdfs快照目录
    public static final String CHECKPOINT_LOCATION = "checkpointLocation";

    //节点类型
    public static final String NODE_INPUT = "input";
    public static final String NODE_EXECUTE = "execute";
    public static final String NODE_OUTPUT = "output";
    public static final String NODE_STATICINPUT = "staticinput";

    //资源类型
    public static final String RESOURCE_KAFKA = "kafka";
    public static final String RESOURCE_ES = "es";
    public static final String RESOURCE_JDBC = "jdbc";
    public static final String RESOURCE_FILE = "file";

    //执行引擎类型
    public static final String EXECUTE_SQL = "sql";
    public static final String EXECUTE_SCALA = "scala";
    public static final String EXECUTE_PYTHON = "python";
    public static final String EXECUTE_JAVA = "java";

    //dataframe json转换参数
    public static final String NEST_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'";
    public static Map<String, String> JSON_OPTIONS = new HashMap<String, String>();
    static {
        JSON_OPTIONS.put("timestampFormat", JavaConstantUtils.NEST_TIMESTAMP_FORMAT);
    }

    //计算引擎
    public static final String CALC_ENGINE_SPARK = "spark";
    public static final String CALC_ENGINE_FLINK = "flink";

    //容错方式
    public static final String FAULT_TOLERANT_SNAPSHOT = "snapshot";
    public static final String FAULT_TOLERANT_HBASE = "hbase";
    public static final String FAULT_TOLERANT_REDIS = "redis";

    //job parameter
    public static final String TRIGGER_UNIT_SECOND = "second";
    public static final String TRIGGER_UNIT_SECONDS = "seconds";

    //UDF 函数类型
    public static final String UDF_TYPE_SCALAR ="ScalarFunction";
    public static final String UDF_TYPE_TABLE ="TableFunction";
    public static final String UDF_TYPE_AGGREGATE ="AggregateFunction";

}
