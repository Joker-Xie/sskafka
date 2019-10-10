package com.meicloud.flink.udf;

import com.meicloud.flink.JavaConstantUtils;
import com.meicloud.spark.entity.CaseVo.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import scala.Tuple2;
import scala.collection.Iterator;


public class UdfUtils {

    /**
     * 获取需要注册的udf函数的相关参数
     *
     * @param configMap
     * @param flink
     */
    public static void registerUDF(Tuple2<String, ExecutorConfigVo> configMap, StreamTableEnvironment flink) {
        switch (configMap._1) {
            case JavaConstantUtils.EXECUTE_SQL:
                ExeSqlConfigVo exeSqlConfigVo = (ExeSqlConfigVo) configMap._2;
                Iterator iter = exeSqlConfigVo.udfList().iterator();
                while (iter.hasNext()) {
                    UDFConfigVo vo = (UDFConfigVo) iter.next();
                    registerUDF(vo.classMethod(), vo.classPath(), exeSqlConfigVo.sqlContent(), flink);
                }
                break;
            case JavaConstantUtils.EXECUTE_SCALA:
                break;
            case JavaConstantUtils.EXECUTE_PYTHON:
                break;
        }
    }


    /**
     * 使用反射的方法获取相关udf实例并按照udf的函数类型完成
     * udf函数注册
     *
     * @param classMethod
     * @param fullClassName
     * @param sqlContent
     * @param flink
     */
    private static synchronized void registerUDF(String classMethod, String fullClassName, String sqlContent, StreamTableEnvironment flink) {
        try {
            Class clazz = Class.forName(fullClassName);
            String superClassName = getSouperClassName(clazz);
            switch (superClassName) {
                case JavaConstantUtils.UDF_TYPE_SCALAR:
                    registerScalarUDF(flink, classMethod, clazz);
                    break;
                case JavaConstantUtils.UDF_TYPE_TABLE:
                    registerTableUDF(flink, classMethod, clazz);
                    break;
                case JavaConstantUtils.UDF_TYPE_AGGREGATE:
                    registerAggrateUDF(flink, classMethod, clazz);
                default:
                    System.err.println("UDF function is mismatching!");
                    System.exit(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TableFunction 类型的udf函数的注册方法
     *
     * @param flink
     * @param classMethod
     * @param clazz
     */
    private static void registerTableUDF(StreamTableEnvironment flink, String classMethod, Class clazz) throws Exception {
        TableFunction function = (TableFunction) clazz.newInstance();
        flink.registerFunction(classMethod, function);
    }

    /**
     * AggregateFunction 类型的udf函数注册方法
     *
     * @param flink
     * @param classMethod
     * @param clazz
     */
    private static void registerAggrateUDF(StreamTableEnvironment flink, String classMethod, Class clazz) throws Exception {
        AggregateFunction function = (AggregateFunction) clazz.newInstance();
        flink.registerFunction(classMethod, function);
    }

    /**
     * ScalarFunction 类型的udf函数注册方法
     *
     * @param flink
     * @param classMethod
     * @param clazz
     */
    private static void registerScalarUDF(StreamTableEnvironment flink, String classMethod, Class clazz) throws Exception {
        ScalarFunction function = (ScalarFunction) clazz.newInstance();
        flink.registerFunction(classMethod, function);
    }

    /**
     * 工具方法，获取udf函数父类的方法名称
     *
     * @param clazz
     */
    private static String getSouperClassName(Class clazz) {
        String[] strs = clazz.getSuperclass().getName().split("\\.");
        int targetIndex = strs.length;
        return strs[targetIndex - 1];
    }

    public static void main(String[] args) {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment table = StreamTableEnvironment.create(env, bsSettings);
        registerUDF("test", "com.meicloud.flink.udf.TestUdf", "dasda", table);
    }
}
