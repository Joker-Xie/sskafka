import com.meicloud.spark.config.Kafka2KafkaConfig
//import com.meicloud.spark.stream.{SSKafka2ES, SSKafka2Kafka}
object SSJobStart {
  def main(args: Array[String]): Unit = {

    //判断配置id是否为空
    if (args.length < 1) {
      System.err.println("Usage: SSJobStart <config_id> ")
      System.exit(-1)
    }else if(! args(0).contains("-") || ! args(0).startsWith("kafka2")){
      System.err.println("Usage: SSJobStart <合法的config_id> ")
      System.exit(-2)
    }
    // 任务id
    val configId = args(0)
    val streamType=configId.split("-")(0)
    /** 第一步 查询任务表 ，配置相关的输入源 输出源和执行引擎类型
      * 1.可以获取执行引擎类型 微批处理、持续处理
      * 2.可以获取输入源和输出源的资源id
      */
//      configId match {
//        case "kafka2es-*" => SSKafka2ES.start(Kafka2ESConfig.getConfig(configId))
//        case _ => System.out.println("暂时不支持此类型流处理")
//      }
//    System.out.println(configId);

    streamType match {
      case "kafka2es" => SSKafka2ES.start(Kafka2ESConfig.getConfig(configId))
      case "kafka2kafka" => SSKafka2Kafka.start(Kafka2KafkaConfig.getConfig(configId))
      case _ => System.out.println("暂时不支持此类型流处理")
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
      *  1、redis
      *  2、hbase
      *  3、默认采用checkpoint和wal保障端对端语义一致性
      */
  }

}
