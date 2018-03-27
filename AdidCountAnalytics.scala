package Spark_Kafka_Redis.SparkStreaming

import Spark_Kafka_Redis.Redis.RedisClient
import Spark_Kafka_Redis.Utill.LogParse
import com.alibaba.fastjson.{JSON, JSONObject}
import cpq.Bean.Constants
import cpq.Utull.ConfigurationScala
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * Created by Administrator on 2017/10/30.
  */
object AdidCountAnalytics {
  def main(args: Array[String]): Unit = {
//    var masterUrl = "local[1]"
//    if (args.length > 0) {
//      masterUrl = args(0)
//    }
    // Create a StreamingContext with the given master URL
//    val conf = new SparkConf().setMaster("local").setAppName("JSTX_Log_Test")
    val conf = new SparkConf().setAppName("Log_Test")
    val ssc = new StreamingContext(conf, Seconds(30))
    // Kafka configurations
    val topics = Set(ConfigurationScala.getString(Constants.KAFKA_TOPICS))
    val brokers =ConfigurationScala.getString(Constants.KAFKA_METADATA_BROKER_LIST)
    val kafkaParams = Map[String, String](
      "zookeeper.session.timeout.ms"->"40000",
      "metadata.broker.list"-> brokers,
      "group.id"->"test_spark",
      "client.id" ->"sparkStraming",
      "serializer.class" -> "kafka.serializer.StringEncoder")

    val dbIndex = 1

    val clickHashKey = "app::adid::num"
    println("************************开始创建createDirectStream**************")

    // Create a direct stream

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    println("************************创建createDirectStream 完成**************")
    var data=new JSONObject()

    val events = kafkaStream.flatMap(line => {
      val json: Option[JSONObject] = LogParse.logToJson(line._2)
      json match {
        case Some(json) => {
          data = json.asInstanceOf[JSONObject]
        }
        case None => println("解析出错!")
      }
//      val data: JSONObject = JSON.parseObject(line._2)
//      val data = JSONObject.fromObject(line._2)
      Some(data)
    })
    // Compute user click times
    val userClicks = events.map(x => (LogParse.getAddid(x),1)).filter((_._1 != "")).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          val adid = pair._1
          val clickCount = pair._2
            println(pair._1+"----------------------"+pair._2)
          val jedis =RedisClient.pool.getResource
          jedis.select(dbIndex)
          jedis.hincrBy(clickHashKey, adid, clickCount.toLong)
          println("写入Redis***************************")
          jedis.close()
//          RedisClient.pool.returnResource(jedis)
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()

  }
}
