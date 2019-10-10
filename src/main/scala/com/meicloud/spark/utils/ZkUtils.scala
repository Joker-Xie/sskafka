package com.meicloud.spark.utils

/**
  * Created by yesk on 2019-3-4. 
  */
object ZkUtils {
  def main(args: Array[String]): Unit = {
    println(getPartitionsForTopics("test.spark.stream.kafka.11"))
  }
  def getPartitionsForTopics(topicName:String): Int ={

    import org.apache.curator.retry.RetryNTimes
    val retryPolicy = new RetryNTimes(3, 1000)
    import org.apache.curator.framework.CuratorFrameworkFactory
    val zkUrl = PropertiesScalaUtils.getString("zookeeper.url")
    val client = CuratorFrameworkFactory.newClient(zkUrl, 10000, 10000, retryPolicy)
    client.start
    //获取当前所有消费者组
    val grouplist = client.getChildren.forPath("/brokers/topics/"+topicName+"/partitions")

    client.close()
    grouplist.size()
  }

}
