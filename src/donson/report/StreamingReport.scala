package donson.report

import com.mongodb.hadoop.MongoOutputFormat
import donson.common.{ReportUtils, SetApplicationLoggerLevel}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.BasicBSONObject

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
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val sparkConf = new SparkConf().setAppName("StreamingSQL").setMaster("local[2]") //spark://cluster01:7077
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    /*ssc.checkpoint("hdfs://master:9000/ck6")*/


    val mongoConf = new Configuration()
    //  mongoConf.set("mongo.input.uri","mongodb://192.168.1.253:27017/db.input")
    mongoConf.set("mongo.output.uri","mongodb://192.168.1.253:27017/db.output4") // 写入db的output2中

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines.filter(_.split(",").length >= 45).foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      val schema = ReportUtils.makeSchemaInfomentions
      val rowRDD = rdd.map(_.split(",")).map(ReportUtils.makeDataToObject)
      val adInfoTable = sqlContext.createDataFrame(rowRDD, schema)

      adInfoTable.registerTempTable("adloginfo")
      sqlContext.cacheTable("adloginfo")

      // 报表 DSP_AssHourlyDetailDataReport 示例
      val results = sqlContext.sql(ReportUtils.DSP_AssHourlyDetailDataReport)
      val saveRDD  = results.map(
        rdata => {
          val bson = new BasicBSONObject()
          bson.put("AdvertisersID", rdata.getAs[String]("AdvertisersID"))
          bson.put("ADOrderID", rdata.getAs[String]("ADOrderID"))
          bson.put("ADCreativeID", rdata.getAs[String]("ADCreativeID"))
          bson.put("ReqDate", rdata.getAs[String]("ReqDate"))
          bson.put("ReqHour", rdata.getAs[String]("ReqHour"))
          bson.put("IsShowSum", rdata.getAs[String]("IsShowSum"))
          bson.put("IsClickSum", rdata.getAs[String]("IsClickSum"))
          bson.put("IsTakeBidSum", rdata.getAs[String]("IsTakeBidSum"))
          bson.put("IsSuccessBidSum", rdata.getAs[String]("IsSuccessBidSum"))
          bson.put("PaymentSum", rdata.getAs[String]("PaymentSum"))
          bson.put("costSum", rdata.getAs[String]("costSum"))
          bson.put("IndependentAudienceSum", rdata.getAs[String]("IndependentAudienceSum"))
          bson.put("IndependentAudienceclickAudienceSum", rdata.getAs[String]("IndependentAudienceclickAudienceSum"))
          (null, bson)
      })
      // file:///bogus无实际意义
      saveRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[MongoOutputFormat[Any, Any]], mongoConf)


      /**
       * other report code
       */

      sqlContext.uncacheTable("adloginfo")
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
