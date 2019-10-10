package com.meicloud.spark.stream

import com.meicloud.spark.config.StreamKafkaConfig
import com.meicloud.spark.config.util.ConfigUtils
import com.meicloud.spark.utils.ConstantUtils

object SSJobStart {
  def main(args: Array[String]): Unit = {
    //判断配置id是否为空
    if (args.length < 1) {
      System.err.println("Usage: SSJobStart <job_name> ")
      System.exit(-1)
    }
    // 任务名称
    val jobName = args(0)
    /** 第一步 查询任务表 ，配置相关的输入源 输出源和执行引擎类型
      * 1.可以获取执行引擎类型 微批处理、持续处理
      * 2.可以获取输入源和输出源的资源id
      */
    val (calcEngine, sourceType) = ConfigUtils.getJobAndInputConfigVo(jobName)
    if (null == calcEngine) {
      System.err.println(jobName + "此任务不存在或者正在启动中")
      System.exit(-1)
    }

    calcEngine.trim.toLowerCase() match {
      case ConstantUtils.CALC_ENGINE_SPARK => { // spark engine
        sourceType match {
          case ConstantUtils.RESOURCE_KAFKA => SSKafka.start(StreamKafkaConfig.getNodeConfigVo(jobName))
          //TODO 加入文本源
          case _ => System.out.println("非法流任务")
        }
      }
      //TODO
      case ConstantUtils.CALC_ENGINE_FLINK => println("期待下一版本能加入Flink") // flink engine
    }
    /** 第二步 根据输入源，输出源id查询相关参数
      * 1.同时根据输入源id和输出源id获取资源参数
      */

    /** 第三步 根据输入源，查询输入配置参数
      * 1.根据输入源参数拼凑spark.read/readStream 读取输入源的方式
      * 2.根据输出源参数拼凑spark.write/writeStream 写入输出源
      */

    /** 第四部，注意关闭各种对象
      *
      */

    /** 第五部，对kafka的offset是否需要第三方存储进行语义完全一次，
      * 本版本暂时采用默认，无需考虑
      * 1、redis
      * 2、hbase
      * 3、默认采用checkpoint和wal保障端对端语义一致性
      */
  }

}
