package com.bfd.api.streaming.scala

import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.slf4j.LoggerFactory

/**
  * Created by jiangnan on 18/1/7.
  */
object KafkaFileTest {
  val logger = LoggerFactory.getLogger("info")

  def main(args: Array[String]) {
//    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local")
  val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(50))
//    ssc.checkpoint("checkpoint")
    var tt= args{0}
    tt = tt.trim()
    var table = args{1}
    var group = args{2}
    group = group.trim()
    var topic = args{3}
    topic = topic.trim()

    val filePath ="ip.txt"
    val props = new Properties()

    props.load(new FileInputStream(filePath))

    val lines = KafkaUtils.createStream(ssc, "bdosn3:2181,bdosn1:2181,bdosn2:2181/kafka", group, Map[String, Int](topic -> 1)).map(_._2)
    lines.foreachRDD((x: RDD[String],time: Time) =>{
      x.foreachPartition{res =>
      {
        if(!res.isEmpty){
          val connection = ConnectionPool.getConnection.getOrElse(null)

          res.foreach {
            r: String =>
              val json = JSON.parseObject(r)
              if(tt.equals(json.get("STATUS"))){
                val userstatqusql = "insert into "+table+" (kafka_value) values (?)"
                val userqustmt = connection.prepareStatement(userstatqusql);
                userqustmt.setString(1,r );
                val userquCount = userqustmt.executeUpdate();
              }
              var ip = props.getProperty("ip")
              if(ip!=null){
                if(ip.equals(json.get("ip"))){
                  val userstatqusql = "insert into "+table+" (kafka_value) values (?)"
                  val userqustmt = connection.prepareStatement(userstatqusql);
                  userqustmt.setString(1,r );
                  val userquCount = userqustmt.executeUpdate();
                }
              }

          }
          ConnectionPool.closeConnection(connection)
        }
      }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
  def write(w:String): Unit ={
    val connection = ConnectionPool.getConnection.getOrElse(null)
            val userstatqusql = "insert into kafka_to_spark (kafka_value) values (?)"
            val userqustmt = connection.prepareStatement(userstatqusql);
            userqustmt.setString(1,w );
            val userquCount = userqustmt.executeUpdate();
//    return 1;
  }

}
