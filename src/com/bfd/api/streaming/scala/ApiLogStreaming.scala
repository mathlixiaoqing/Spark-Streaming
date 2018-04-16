package com.bfd.api.streaming.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.slf4j.LoggerFactory
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * @author BFD_512
 */
object ApiLogStreaming {
  def main(args: Array[String]) {
    count()
  }
  val  infologger = LoggerFactory.getLogger("info");
  val  errorlogger = LoggerFactory.getLogger("error");
  val  warnlogger = LoggerFactory.getLogger("warn");
  def count() {

    infologger.info("starting...")
    // 加载server配置文件
    val osName = System.getProperties().getProperty("os.name")
//    var path = "resources/sparkstreaming-conf.properties"
//    var path = "/Users/jiangnan/Documents/work/baifendian/BD-OSV3.0/bdos_project/api-scala/src/sparkstreaming-conf.properties"
    if (osName.indexOf("linux") >= 0 || osName.indexOf("Linux") >= 0 || osName.indexOf("LINUX") >= 0) {
//      path = "sparkstreaming-conf.properties";

    }

//    val properties = new Properties()
//    properties.load(new FileInputStream(path))
    var master = "local"
    infologger.info("master=" + master)

    var appName = "bbc"
    infologger.info("appName=" + appName)

    var secondsStr = "10"
    infologger.info("secondsStr=" + secondsStr)

    var seconds = java.lang.Long.valueOf(secondsStr)

//    var zkAddress = ""
//    infologger.info("zkAddress=" + zkAddress)

    var topicName = "apache-logs"
    infologger.info("topicName=" + topicName)

    // 从配置文件中读取时间间隔、zk地址、消费group、Topic、线程数、Spark部署模式、AppName
    val sparkConf = new SparkConf().setMaster(master).setAppName("a")
//    val sparkConf = new SparkConf()
    sparkConf.set("spark.ui.enabled", "false");
    // 每30秒一个批次
    val ssc = new StreamingContext(sparkConf, Seconds(seconds))

    // 这个消费的group名称为userapicountgroup，不能随便改动，也不要做成配置，默认即可，否则会有重复 消费问题
    // topic的名称就是access，建议也不要随便改动
    try {
      val kafkaStream = KafkaUtils.createStream(ssc, "bdosn3:2181,bdosn1:2181,bdosn2:2181/kafka", "jnbbc21", Map[String, Int](topicName -> 1)).map(x => x._2)
      kafkaStream.foreachRDD((rdd: RDD[String], time: Time) => {

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._
        // 构造case class: apiLog,提取日志中相应的字段
        val logDataFrame = rdd.map(w => ApiLog(w)).toDF()
        //注册为tempTable
        logDataFrame.registerTempTable("logtable")

        // user维度统计
        // t_user_stat
        // t_user_day_stat
        // t_day_stat
        // t_top_stat
        val userCountsDataFrame = sqlContext.sql(" select day from logtable  ")
        userCountsDataFrame.collect();

        val userCountsRowRDD = userCountsDataFrame.rdd
        userCountsRowRDD.foreachPartition { partitionOfRecords =>
          {
            if (partitionOfRecords.isEmpty) {
              println("this is RDD(userCountsRowRDD)  is not null but partition is null")
            } else {
              val connection = ConnectionPool.getConnection.getOrElse(null)
              partitionOfRecords.foreach(record => {

                try {

                  /**
                   * 日期处理
                   */
                  val daystr = record.getAs("day").toString();
                  if(daystr.contains("404")){

                  }
                  val userstatqusql = "insert into kafka_to_spark (kafka_value) values (?)"
                  val userqustmt = connection.prepareStatement(userstatqusql);
                  userqustmt.setString(1, daystr);
                  val userquCount = userqustmt.executeUpdate();
                  System.out.print(daystr)

                } catch {
                  case e: Exception => errorlogger.error("exception caught: " + e);
                }

              })
              ConnectionPool.closeConnection(connection)
            }
          }
        }
      })

    } catch {
      case e: Exception => errorlogger.error("exception caught: " + e);
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
//数据格式 20170220 testapi depo 128
// apiname apiurl apiprefixname username userId flow
case class ApiLog(day: String)
object SQLContextSingleton {
  @transient private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}