package com.meicloud.spark.config

import java.util

import com.meicloud.spark.config.util.ConfigUtils
import com.meicloud.spark.entity.CaseVo.{ExecutorConfigVo, _}
import com.meicloud.spark.utils.ConstantUtils

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * Created by yesk on 2019-1-28. 
  */
object StreamKafkaConfig {
  def main(args: Array[String]): Unit = {
    System.out.println(getNodeConfigVo("kafka2es-car-01"))
  }

  def getNodeConfigVo(jobName: String):
  (JobConfigVo, (String, InputSourceConfigVo), (String, ExecutorConfigVo),
    mutable.HashMap[String, OutputSourceConfigVo], mutable.HashMap[String, StaticInputSourceConfigVo]) = {
    val jobConfigVo = ConfigUtils.getJobConfigVo(jobName)
    var inputSourceConfigVoTuple: (String, InputSourceConfigVo) = null
    val staticInputSourceConfigVoMap = mutable.HashMap[String, StaticInputSourceConfigVo]()
    var executorConfigVoTuple: (String, ExecutorConfigVo) = null

    //节点信息
    val list: java.util.ArrayList[(String, String, String)] = ConfigUtils.getNodeConfigVo(jobName)
    val outputSourceConfigVoMap = mutable.HashMap[String, OutputSourceConfigVo]()

    list.foreach { line: (String, String, String) => {
      val nodeType = line._1.trim
      val sourceType = line._2.trim
      val sourceId = line._3.trim
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      println("nodeType:" + nodeType + ",sourceType:" + sourceType + ",sourceId:" + sourceId)
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

      nodeType match {
        case ConstantUtils.NODE_INPUT => {
          sourceType match {
            case ConstantUtils.RESOURCE_KAFKA => {
              inputSourceConfigVoTuple = (ConstantUtils.RESOURCE_KAFKA, ConfigUtils.getInputKafkaConfig(sourceId))
            }
          }
        }
        case ConstantUtils.NODE_EXECUTE => {
          sourceType match {
            case ConstantUtils.EXECUTE_SQL => {
              executorConfigVoTuple = (ConstantUtils.EXECUTE_SQL, ConfigUtils.getExecuteSqlConfig(sourceId))
            }
            case _ => {}
          }
        }
        case ConstantUtils.NODE_OUTPUT => {
          sourceType match {
            case ConstantUtils.RESOURCE_ES => {
              outputSourceConfigVoMap.put(ConstantUtils.RESOURCE_ES, ConfigUtils.getOutputEsConfig(sourceId))
            }
            case ConstantUtils.RESOURCE_JDBC => {
              outputSourceConfigVoMap.put(ConstantUtils.RESOURCE_JDBC, ConfigUtils.getOutputJdbcConfig(sourceId))
            }
            case ConstantUtils.RESOURCE_FILE => {
              outputSourceConfigVoMap.put(ConstantUtils.RESOURCE_FILE, ConfigUtils.getOutputFileConfig(sourceId))
            }
            case ConstantUtils.RESOURCE_KAFKA => {
              outputSourceConfigVoMap.put(ConstantUtils.RESOURCE_KAFKA, ConfigUtils.getOutputKafkaConfig(sourceId))
            }
            case _ => {}
          }
        }
        case ConstantUtils.NODE_STATICINPUT => {
          val list = ConfigUtils.getStaticInputJdbcConfig(sourceId)
          for (i <- 0 until list.size()) {
            val row = list.get(i)
            row.sourceType match {
              case ConstantUtils.RESOURCE_JDBC => {
                staticInputSourceConfigVoMap.put(ConstantUtils.RESOURCE_JDBC+"_"+i, row)
              }
              case ConstantUtils.RESOURCE_KAFKA => {
                //              staticInputSourceConfigVoMap = (ConstantUtils.RESOURCE_KAFKA,ConfigUtils.getStaticInputKafkaConfig(sourceId))
              }
            }
          }

        }
        case _ => {}
      }

    }

    }
    (jobConfigVo, inputSourceConfigVoTuple, executorConfigVoTuple, outputSourceConfigVoMap, staticInputSourceConfigVoMap)

  }


}
