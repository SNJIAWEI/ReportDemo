package donson.report

import java.util.Properties

import donson.common.{SetApplicationLoggerLevel, ReportUtils}
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingReport {

  def main(args: Array[String]) {
    SetApplicationLoggerLevel.setLevels(Level.WARN)
//    if (args.length != 4) {
//      System.err.println("Usage: StreamingSQL <dbUrl> <tableName> <user> <password> <filePath>")
//      System.exit(1);
//    }

    //    val Array(zkQuorum, group, topics, numThreads, outurl) = args
    val zkQuorum = "192.168.1.221:2181,192.168.1.222:2181,192.168.1.223:2181"
    val group = "test-group"
    val topics = "testTopic1"
    val numThreads = 1

    // 配置数据库连接参数
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "root")
    connectionProperties.setProperty("password", "123456")
    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    val sparkConf = new SparkConf().setAppName("StreamingSQL").setMaster("local[2]") //spark://cluster01:7077
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    /*ssc.checkpoint("hdfs://master:9000/ck6")*/

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines.filter(_.split(",").length >= 45).foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      val schema = ReportUtils.makeSchemaInfomentions

      val rowRDD = rdd.map(_.split(",")).map(ReportUtils.makeDataToObject)
      val rddDataFrame = sqlContext.createDataFrame(rowRDD, schema)


      rddDataFrame.write.parquet("hdfs://192.168.1.220:9000/dshbase/report_ch_" + ReportUtils.formateFileName + ".parquet")


     /* val parquetFile = sqlContext.read.parquet("hdfs://192.168.1.220:9000/donson/streaming/report_1_" + ReportUtils.formateFileName + ".parquet")
      parquetFile.registerTempTable("adloginfo")
      // cache table
//      sqlContext.cacheTable("adloginfo")


      // 查询结果并存储到数据库中
      val results = sqlContext.sql("SELECT AdvertisersID,ADOrderID,ADCreativeID,ReqDate,ReqHour,sum(IsShow),sum(IsClick),sum(IsTakeBid),sum(IsSuccessBid)," +
        "sum(Payment),sum(cost),sum(IndependentAudience),sum(IndependentAudienceclickAudience) " +
        "FROM adloginfo " +
        "group by AdvertisersID,ADOrderID,ADCreativeID,ReqDate,ReqHour")
      // results.collect().foreach(println)
      results.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.130:3306/test", "report_1", connectionProperties)*/
      // uncache table
//      sqlContext.uncacheTable("adloginfo")
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
