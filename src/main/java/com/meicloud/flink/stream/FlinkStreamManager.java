package com.meicloud.flink.stream;

import com.meicloud.flink.asynstream.JdbcAsynStream;
import com.meicloud.flink.connector.KafkaConnetor;
import com.meicloud.flink.shceme.utils.SchemaUtils;
import com.meicloud.flink.stream.utils.CkeckPointBuild;
import com.meicloud.flink.stream.utils.MysqlUtils;
import com.meicloud.flink.stream.utils.SQLParserUtils;
import com.meicloud.flink.table.sink.SinkUtils;
import com.meicloud.flink.table.source.CriterStreamTableFactoryBase;
import com.meicloud.flink.table.source.LocalKafkaTableSource;
import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;


import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author xiehy11
 */
public class FlinkStreamManager {


    /**
     * 任务执行器
     *
     * @param jobConfigVo
     * @param inputKafkaConfigVo
     * @param executorConfigVoTuple
     * @param outputSourceConfigVoMap
     * @param staticInputSourceConfigVoMap
     * @param flink
     */
    public static void kafkaStreamDealt(JobConfigVo jobConfigVo, InputKafkaConfigVo inputKafkaConfigVo, Tuple2<String, ExecutorConfigVo> executorConfigVoTuple, HashMap<String, OutputSourceConfigVo> outputSourceConfigVoMap, HashMap<String, StaticInputSourceConfigVo> staticInputSourceConfigVoMap, Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink) {
        // 使用RocksDB作为状态后端管理chepoint点,并将Chcketpoint点写入HDFS进行管理
        CkeckPointBuild.setRocksDBStateBackend(flink, jobConfigVo);
        // 第一步 获取输入源数据链接器
        StreamTableSource sourceStream = FlinkStreamManager.getInputSource(inputKafkaConfigVo);
        // 第二步 根据数据更新方式对链接器进行注册
        tableRegister(sourceStream, jobConfigVo.outputMode(), flink, executorConfigVoTuple, staticInputSourceConfigVoMap);
        // 第三步 逻辑处理层，对输入源数据sql或者其他逻辑处理返回kafka里面的数据
        Table table = executor(executorConfigVoTuple, flink);
        // 第四步 注册指定的Sink
        loadResult2TargetSink(table, outputSourceConfigVoMap, flink);
        // 弟五步 执行程序
        startApplication(flink);
    }

    /**
     * 程序开始执行
     *
     * @param flink
     */
    private static void startApplication(Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink) {
        StreamExecutionEnvironment env = flink.f0;
        try {
            env.execute("Flink Job Start.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 将结果集写出目标Sink
     *
     * @param outputSourceConfigVoMap 目标sink对象
     */
    private static void loadResult2TargetSink(Table table, HashMap<String, OutputSourceConfigVo> outputSourceConfigVoMap, Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink) {
        Iterator itor = outputSourceConfigVoMap.keySet().iterator();
        StreamTableEnvironment evTable = flink.f1;
        while (itor.hasNext()) {
            String sinkType = itor.next().toString();
            switch (sinkType) {
                case "kafka":
                    OutputKafkaConfigVo outputKafkaConfigVo = (OutputKafkaConfigVo) outputSourceConfigVoMap.get(sinkType);
                    SinkUtils.load2KafkaSink(table, outputKafkaConfigVo, evTable);
                    break;
                case "jdbc":
                    OutputJdbcConfigVo outputJdbcConfigVo = (OutputJdbcConfigVo) outputSourceConfigVoMap.get(sinkType);
                    SinkUtils.load2JdbcSink(outputJdbcConfigVo, evTable);
                    break;
                case "es":
                    OutputEsConfigVo outputEsConfigVo = (OutputEsConfigVo) outputSourceConfigVoMap.get(sinkType);
                    SinkUtils.load2EsSink(outputEsConfigVo, evTable);
                    break;
                case "file":
                    OutputFileConfigVo outputFileConfigVo = (OutputFileConfigVo) outputSourceConfigVoMap.get(sinkType);
                    SinkUtils.load2FileSink(outputFileConfigVo, evTable);
                    break;
                default:
                    System.err.println("outputSourceConfigVoMap is not support!");
                    System.exit(-1);
            }
        }

    }

    /**
     * 执行SQL语句获得Table对象返回
     *
     * @param executorConfigVoTuple 配置参数类
     * @param flink                 执行器类
     */
    private static Table executor(Tuple2<String, ExecutorConfigVo> executorConfigVoTuple, Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink) {
        StreamTableEnvironment table = flink.f1;
        JavaExeSqlConfigVo exeSqlConfigVo = (JavaExeSqlConfigVo) executorConfigVoTuple.f1;
        String sql = exeSqlConfigVo.sqlContent();
        Table targeTable = table.sqlQuery(sql);
        return targeTable;
    }

    /**
     * @param tableSource
     * @param outputMode
     * @param flink
     * @param executorConfigVoTuple
     * @param staticInputSourceConfigVoMap
     */
    private static void tableRegister(StreamTableSource tableSource, String outputMode, Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink, Tuple2<String, ExecutorConfigVo> executorConfigVoTuple, HashMap<String, StaticInputSourceConfigVo> staticInputSourceConfigVoMap) {
        //注册静态数据的表并获取静态数据表名称
        ArrayList<String> staticTableNames = staticDataStreamRegister(staticInputSourceConfigVoMap, flink);
        //使用druid对SQL进行解析拿到表名称
        JavaExeSqlConfigVo exeSqlConfigVo = (JavaExeSqlConfigVo) executorConfigVoTuple.f1;
        String sql = exeSqlConfigVo.sqlContent();
        String streamTableName = SQLParserUtils.parserSQLAndGetTableNames(sql, staticTableNames);
        //获取数据更新模式并将数据注册进表格中
        flink.f1.registerTableSource(streamTableName, tableSource);
    }

    /**
     * @param staticInputSourceConfigVoMap
     */
    private static ArrayList<String> staticDataStreamRegister(HashMap<String, StaticInputSourceConfigVo> staticInputSourceConfigVoMap, Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink) {
        ArrayList<String> staticTableNames = new ArrayList<String>();
        Iterator iterator = staticInputSourceConfigVoMap.keySet().iterator();
        //TODO 缓存使用方式有待讨论
        Cache<String, ResultSet> cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(2, TimeUnit.SECONDS)
                .build();
        while (iterator.hasNext()) {
            String staticType = iterator.next().toString();
            String model = staticType.split("_")[0];
            switch (model) {
                case "kafka":
                    KafkaStaticConfVo kafkaStaticConfVo = (KafkaStaticConfVo) staticInputSourceConfigVoMap.get(staticType);
                    String registerTableName = kafkaStaticConfVo.tableName();
                    staticTableNames.add(registerTableName);
                    kafkaStaticStreamLoadAndRegister(kafkaStaticConfVo, registerTableName, flink);
                    break;
                case "jdbc":
                    JdbcStaticConfVo jdbcStaticConfVo = (JdbcStaticConfVo) staticInputSourceConfigVoMap.get(staticType);
                    String registerJdbcTableName = jdbcStaticConfVo.registTableName();
                    staticTableNames.add(jdbcStaticConfVo.registTableName());
                    jdbcStaticStreamLoadAndRegister(jdbcStaticConfVo, registerJdbcTableName, flink, cache);
                    break;
                case "hive":
                    HiveStaticConfVo hiveStaticConfVo = (HiveStaticConfVo) staticInputSourceConfigVoMap.get(staticType);
                    String registerHiveTableName = hiveStaticConfVo.hiveTableName();
                    staticTableNames.add(hiveStaticConfVo.registTableName());
                    hiveStaticStreamLoadAndRegister(hiveStaticConfVo, registerHiveTableName, flink);
                    break;
                case "file":
                    break;
                case "es":
                    EsStaticConfVo esStaticConfVo = (EsStaticConfVo) staticInputSourceConfigVoMap.get(staticType);
                    String registerEsTableName = esStaticConfVo.tableName();
                    staticTableNames.add(registerEsTableName);
                    esStaticStreamLoadAndRegister(esStaticConfVo, registerEsTableName, flink);
                    break;
                default:
                    System.err.println("暂未支持该类型");
            }
        }
        return staticTableNames;
    }

    //TODO ES 数据静态加载
    private static void esStaticStreamLoadAndRegister(EsStaticConfVo esStaticConfVo, String registerEsTableName, Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink) {

    }

    //TODO Hive 数据静态加载
    private static void hiveStaticStreamLoadAndRegister(HiveStaticConfVo hiveStaticConfVo, String registerHiveTableName, Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink) {

    }

    private static void jdbcStaticStreamLoadAndRegister(JdbcStaticConfVo jdbcStaticConfVo, String registerJdbcTableName, Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink, Cache<String, ResultSet> cache) {
        StreamTableEnvironment table = flink.f1;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JdbcAsynStream jdbcAsynStream = new JdbcAsynStream(jdbcStaticConfVo, cache);
        DataStream<String> source = env.fromElements("1");
        SingleOutputStreamOperator<Row> dataStream = AsyncDataStream.orderedWait(source, jdbcAsynStream, 2, TimeUnit.MINUTES);
        String fields = new MysqlUtils(jdbcStaticConfVo).getDBMetaInfo().f0;
        table.registerDataStream(registerJdbcTableName, dataStream, fields);
    }

    //TODO Kafka 数据静态加载
    private static void kafkaStaticStreamLoadAndRegister(KafkaStaticConfVo kafkaStaticConfVo, String registerTableName, Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> flink) {
        StreamTableEnvironment table = flink.f1;
        table.registerTableSource("test", new TableSource<String>() {
            @Override
            public TableSchema getTableSchema() {
                return null;
            }
        });
    }

    private static StreamTableSource getInputSource(InputKafkaConfigVo inputConfigVo) {
        if (inputConfigVo != null) {
            return new CriterStreamTableFactoryBase(inputConfigVo).getTableSource();
        }
        return null;
    }
}
