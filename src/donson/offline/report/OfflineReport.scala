package donson.offline.report

import java.net.URI

import com.mongodb.hadoop.MongoOutputFormat
import donson.common.{ReportUtils, SetApplicationLoggerLevel}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BasicBSONObject

/**
 * Created by lomark on 15/9/15.
 * 离线处理hadoop集群上的文件，处理完成后将文件修改为xxxx.done
 */
object OfflineReport {

  def main(args: Array[String]) {
    //    if (args.length != 4) {
    //      System.err.println("Usage: SparkSqlDemo <dbUrl> <tableName> <user> <password> <filePath>")
    //      System.exit(1);
    //    }
    // Set Application logs level
    SetApplicationLoggerLevel.setLevels(Level.WARN)

    val conf = new Configuration()
    val fs = FileSystem.get(new URI("hdfs://192.168.1.220:9000"),conf,"hadoop")
    val filesStatus  = fs.listStatus(new Path("/donson"))

    val sparkConf = new SparkConf().setAppName("OfflineReport")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val mongoConf = new Configuration()
    //  mongoConf.set("mongo.input.uri","mongodb://192.168.1.253:27017/db.input")
    mongoConf.set("mongo.output.uri","mongodb://192.168.1.253:27017/db.output4") // 写入db的output2中

    filesStatus.foreach{
      fileStatus =>
        val filePath: Path = fileStatus.getPath
        val fileName = filePath.getName
        println("currentFileName : "+fileName)
        if (!filePath.toString.endsWith(".done")) {

          val schemaInfo = ReportUtils.makeSchemaInfomentions
          val adInfoRdd = sc.textFile(filePath.toString).map(_.split(",")).map(ReportUtils.makeDataToObject)
          val adInfoTable = sqlContext.createDataFrame(adInfoRdd, schemaInfo) // 数据和schema信息转化成DataFrame

          adInfoTable.registerTempTable("adloginfo")
          sqlContext.cacheTable("adloginfo")

          // 报表 DSP_AssHourlyDetailDataReport 示例
          val results = sqlContext.sql(ReportUtils.DSP_AssHourlyDetailDataReport)
          val saveRDD  = results.map( rdata => {
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
          fs.rename(filePath, new Path(filePath.toString.concat(".done")))

          /**
           * other report code
           */

          sqlContext.uncacheTable("adloginfo")
        }
    }

    sc.stop()
  }

}
