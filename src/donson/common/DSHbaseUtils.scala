package donson.common

import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
 * Created by lomark on 2015/9/18 0018.
 * 处理用户UUID
 * RowKey 格式: "ASS" + "Click" + AdvertisersID + ADOrderID + ADCreativeID + uuid
 */
object DSHbaseUtils extends Serializable {

  val tableName = "donson"
  val cfName = "cf"
  val cName = ""
  val cValue = ""


  def getHbaseConf() = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "hadoop-server02,hadoop-server03")
    conf.addResource("/usr/local/hbase-site.xml")
    conf
  }

  /**
   * 判断rowkey 是否存在
   */
  def rowKeyIsExist(isClick: Boolean, advertisersID: String, adOrderID: String, adCreativeID: String, uuid: String) = {

    val conf = this.getHbaseConf()
    val rowKey = this.genAudienceKey(isClick, advertisersID, adOrderID, adCreativeID, uuid)
    val table = new HTable(conf, Bytes.toBytes(tableName))
    val get = new Get(rowKey.getBytes)
    val result = table.get(get)
    var isExist: Boolean = true
    if (result.size() == 0) isExist = false
    isExist
  }

  /**
   * 写数据
   */
  def write2Hbase(isClick: Boolean, advertisersID: String, adOrderID: String, adCreativeID: String, uuid: String) = {
    val conf = this.getHbaseConf()
    val rowKey = this.genAudienceKey(isClick, advertisersID, adOrderID, adCreativeID, uuid)

    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      tableDesc.addFamily(new HColumnDescriptor(cfName.getBytes()))
      admin.createTable(tableDesc)
    }
    val table = new HTable(conf, tableName)
    val put = new Put(Bytes.toBytes(rowKey))
    put.add(Bytes.toBytes(cfName), Bytes.toBytes(cName), Bytes.toBytes(cValue))
    table.put(put)
    table.flushCommits()
  }

  /**
   * 新增曝光受众和新增点击受众的 rowkey
   */
  def genAudienceKey(isClick: Boolean, AdvertisersID: String, ADOrderID: String, ADCreativeID: String, uuid: String): String = {
    if (isClick) "ASS" + "Click" + AdvertisersID + ADOrderID + ADCreativeID + uuid
    else "ASS" + AdvertisersID + ADOrderID + ADCreativeID + uuid

  }

}
