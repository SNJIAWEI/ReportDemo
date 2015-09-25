package donson.common

import java.text.SimpleDateFormat
import java.util.{UUID, Date}

import com.donson.hbase.DsHbaseUtils
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HConnectionManager, HTable, Get}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.io.Source
import scala.util.parsing.json.JSON

/**
 * Created by Jiawei on 15/9/15.
 */
object ReportUtils {

  /**
   * 获取广告主的利润率
   */
  private[common] val requestURL: String = null

  /**
   * 生成格式文件名称
   */
  def formateFileName = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
  /**
   * 格式化时间格式为HH
   */
  def formateRequestForHour(str :String) = str.replace("-","").replace(" ","").substring(8,10)

  /**
   * 格式化时间格式为20150911-yyyyMMdd
   */
  def formateRequestForDate(str :String) = str.replace("-","").replace(" ","").substring(0,8)

  /**
   * 数据映射
   */
  def makeDataToObject: (Array[String]) => Row = {
    prop => {
      val sessionID = prop(0).trim
      val advertisersID = prop(1).trim
      val adOrderID = prop(2).trim
      val adCreativeID = prop(3).trim
      val adPlatformProviderID = prop(4).trim
      val SDKVersionNumber = prop(5).trim
      val adPlatformKey = prop(6).trim
      val putInModelType = prop(7).trim
      val requestMode = prop(8).trim
      val adPrice = prop(9).trim
      val adPPPrice = prop(10).trim
      val requestDate = prop(11).trim
      val ip = prop(12).trim
      val appID = prop(13).trim
      val appName = prop(14).trim
      val uuid = prop(15).trim
      val device = prop(16).trim
      val client = prop(17).trim
      val osVersion = prop(18).trim
      val density = prop(19).trim
      val pw = prop(20).trim
      val ph = prop(21).trim
      val long = prop(22).trim
      val lat = prop(23).trim
      val provinceName = prop(24).trim
      val cityName = prop(25).trim
      val isPID = prop(26).trim
      val isPName = prop(27).trim
      val networkMannerID = prop(28).trim
      val networkMannerName = prop(29).trim
      val isEffective = prop(30).trim
      val isBilling = prop(31).trim
      val adSpaceType = prop(32).trim
      val adSpaceTypeName = prop(33).trim
      val deviceType = prop(34).trim
      val processNode = prop(35).trim
      val appType = prop(36).trim
      val district = prop(37).trim
      val payMode = prop(38).trim
      val isBid = prop(39).trim
      val bidPrice = prop(40).trim
      val winPrice = prop(41).trim
      val isWin = prop(42).trim
      val cur = prop(43).trim
      val rate = prop(44).trim
      val cnyWinPrice = prop(45).trim

      Row(
        sessionID, advertisersID, adOrderID, adCreativeID, adPlatformProviderID, SDKVersionNumber, adPlatformKey,
        putInModelType, requestMode, adPrice, adPPPrice, requestDate, ip, appID, appName, uuid, device, client,
        osVersion, density, pw, ph, long, lat, provinceName, cityName, isPID, isPName,networkMannerID, networkMannerName,
        isEffective, isBilling, adSpaceType, adSpaceTypeName, deviceType, processNode, appType, district, payMode, isBid,
        bidPrice, winPrice,isWin, cur, rate, cnyWinPrice,

        // 展示量
        if (!requestMode.isEmpty && requestMode.toInt == 2 && isEffective.equals("1")) 1 else 0,
        // 点击量
        if (!requestMode.isEmpty && requestMode.toInt == 3 && isEffective.equals("1")) 1 else 0,

        // 参与竞价数
        if (!adPlatformProviderID.isEmpty
          && adPlatformProviderID.toInt >= 100000
          && isEffective.equals("1")
          && isBilling.equals("1")
          && isBid.equals("1")) 1 else 0,

        // 竞价成功数
        if (!adPlatformProviderID.isEmpty
          && adPlatformProviderID.toInt >= 100000
          && isEffective.equals("1")
          && isBilling.equals("1")
          && isWin.equals("1")) 1 else 0,

        if (!requestDate.isEmpty) formateRequestForDate(requestDate) else NullType,
        if (!requestDate.isEmpty) formateRequestForHour(requestDate) else NullType,
        // 曝光受众
        if (!requestMode.isEmpty && !isEffective.isEmpty && requestMode.equals("2") && isEffective.equals("1")) 1 else 0,
        // 点击受众
        if (!requestMode.isEmpty && !isEffective.isEmpty && requestMode.equals("3") && isEffective.equals("1")) 1 else 0,
        // 消费
        if (!adPlatformProviderID.isEmpty && !adOrderID.isEmpty &&  !adCreativeID.isEmpty &&
          adPlatformProviderID.toInt >= 100000 &&  adOrderID.toInt > 200000 && adCreativeID.toInt > 200000  &&
          isEffective.equals("1")  && isBilling.equals("1") && isBid.equals("1")) loadAdvertisersPriceMap(advertisersID, winPrice.toDouble) else 0.0,

        // 成本
        if (!adPlatformProviderID.isEmpty && !adOrderID.isEmpty &&  !adCreativeID.isEmpty &&
          adPlatformProviderID.toInt >= 100000 &&  adOrderID.toInt > 200000 && adCreativeID.toInt > 200000  &&
          isEffective.equals("1")  && isBilling.equals("1") && isBid.equals("1")) winPrice.toDouble / 1000 else 0.0,
      if (DSHbaseUtils.rowKeyIsExist(false,advertisersID, adOrderID, adCreativeID, uuid)) 0
      else  {
        DSHbaseUtils.write2Hbase(false,advertisersID,adOrderID,adCreativeID,uuid)
        1
      }
      ,

      if(DSHbaseUtils.rowKeyIsExist(true,advertisersID, adOrderID, adCreativeID,uuid)) 0
      else {
        DSHbaseUtils.write2Hbase(true, advertisersID, adOrderID, adCreativeID, uuid)
        1
      }
      )
    }
  }

  /**
   * 广告主&利润率映射关系
   * @param advertisersId
   * @param winPrice
   * @return
   */
  def loadAdvertisersPriceMap(advertisersId: String, winPrice: Double) : Double = {
    if (winPrice <= 0) 0.0
    else {
      if (null == requestURL || requestURL.isEmpty || null == advertisersId || advertisersId.isEmpty) winPrice/1000
      else {
        JSON.parseFull(Source.fromURL(requestURL).mkString) match {
          case Some(map: Map[String, Double]) => (winPrice / 1000.0) / (1.0 - map.get(advertisersId).getOrElse(winPrice / 1000.0))
          case _ => winPrice / 1000.0
        }
      }
    }
  }

  def genAudienceKey(isClick: Boolean, AdvertisersID: String, ADOrderID: String, ADCreativeID: String, uuid: String): String = {
    if (isClick) "ASS" + "Click" + AdvertisersID + ADOrderID + ADCreativeID + uuid
    else "ASS" + AdvertisersID + ADOrderID + ADCreativeID + uuid
  }

  /**
   * 定义数据结构信息
   */
  def makeSchemaInfomentions: StructType = {
    StructType(StructField("SessionID", StringType, true) // 0
      :: StructField("AdvertisersID", StringType, true) // 1
      :: StructField("ADOrderID", StringType, true) // 2
      :: StructField("ADCreativeID", StringType, true) // 3
      :: StructField("ADPlatformProviderID", StringType, true) // 4
      :: StructField("SDKVersionNumber", StringType, true) // 5
      :: StructField("AdPlatformKey", StringType, true) // 6
      :: StructField("PutInModelType", StringType, true) // 7
      :: StructField("RequestMode", StringType, true) // 8
      :: StructField("ADPrice", StringType, true) // 9
      :: StructField("ADPPPrice", StringType, true) // 10
      :: StructField("RequestDate", StringType, true) // 11
      :: StructField("Ip", StringType, true) // 12
      :: StructField("AppID", StringType, true) // 13
      :: StructField("AppName", StringType, true) // 14
      :: StructField("Uuid", StringType, true) // 15
      :: StructField("Device", StringType, true) // 16
      :: StructField("Client", StringType, true) // 17
      :: StructField("OsVersion", StringType, true) // 18
      :: StructField("Density", StringType, true) // 19
      :: StructField("Pw", StringType, true) // 20
      :: StructField("Ph", StringType, true) // 21
      :: StructField("Long", StringType, true) // 22
      :: StructField("Lat", StringType, true) // 23
      :: StructField("ProvinceName", StringType, true) // 24
      :: StructField("CityName", StringType, true) // 25
      :: StructField("ISPID", StringType, true) // 26
      :: StructField("ISPName", StringType, true) // 27
      :: StructField("NetworkMannerID", StringType, true) // 28
      :: StructField("NetworkMannerName", StringType, true) // 29
      :: StructField("IsEffective", StringType, true) // 30
      :: StructField("IsBilling", StringType, true) // 31
      :: StructField("AdSpaceType", StringType, true) // 32
      :: StructField("AdSpaceTypeName", StringType, true) // 33
      :: StructField("DeviceType", StringType, true) // 34
      :: StructField("ProcessNode", StringType, true) // 35
      :: StructField("AppType", StringType, true) // 36
      :: StructField("District", StringType, true) // 37
      :: StructField("PayMode", StringType, true) // 38
      :: StructField("IsBid", StringType, true) // 39
      :: StructField("BidPrice", StringType, true) // 40
      :: StructField("WinPrice", StringType, true) // 41
      :: StructField("IsWin", StringType, true) // 42
      :: StructField("Cur", StringType, true) // 43
      :: StructField("Rate", StringType, true) // 44
      :: StructField("CnyWinPrice", StringType, true) // 45
      :: StructField("IsShow", IntegerType, true) // 以下为自定义字段标志 是否展示1：是 0：否
      :: StructField("IsClick", IntegerType, true) // 是否点击 1：是 0：否
      :: StructField("IsTakeBid", IntegerType, true) // 是否参与竞价 1：是 0：否
      :: StructField("IsSuccessBid", IntegerType, true) // 是否竞价成功 1：是 0：否
      :: StructField("ReqDate", StringType, true) // 请求是个格式 yyyyMMdd
      :: StructField("ReqHour", StringType, true) // 请求是个格式 HH
      :: StructField("IndependentAudience", IntegerType, true) // 曝光受众
      :: StructField("IndependentAudienceclickAudience", IntegerType, true) // 点击受众
      :: StructField("Payment", DoubleType, true) // 消费
      :: StructField("cost", DoubleType, true)    // 成本
      :: StructField("NewAudience", IntegerType)    // 新增曝光受众
      :: StructField("NewclickAudience", IntegerType)    // 新增点击受众
      :: Nil)
  }


  /**
   * 报表demo
   */
  def DSP_AssHourlyDetailDataReport = {
    s"SELECT AdvertisersID, ADOrderID, ADCreativeID, ReqDate, ReqHour," +
      "sum(IsShow) IsShowSum, " +
      "sum(IsClick) IsClickSum, " +
      "sum(IsTakeBid) IsTakeBidSum, " +
      "sum(IsSuccessBid) IsSuccessBidSum," +
      "sum(Payment) PaymentSum," +
      "sum(cost) costSum," +
      "sum(IndependentAudience) IndependentAudienceSum," +
      "sum(IndependentAudienceclickAudience)  IndependentAudienceclickAudienceSum, " +
      "sum(NewAudience),sum(NewclickAudience)  " +
      "FROM adloginfo " +
      "group by AdvertisersID,ADOrderID,ADCreativeID,ReqDate,ReqHour"
  }

}
