package donson.redis

import java.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis._
import redis.clients.util.Hashing

import scala.collection.mutable

/**
 * Created by Jiawei on 15/9/22.
 */
object RedisUtils {
  var SECONDS: Int = 3600 * 24
  private var pool: JedisPool = null
  private var shardPool: ShardedJedisPool = null
  private var jedisCluster: JedisCluster = null

  //jedis配置
     val config:JedisPoolConfig = new JedisPoolConfig();
    config.setMaxIdle(1000); //最大空闲
    config.setMaxTotal(10240); //最大连接数
    if (pool == null) {
      //config：配置参数； 6379：默认端口号，可以更改；e_learning：密码
      pool = new JedisPool(config, "192.168.1.120", 6379, 0);
//      pool = new JedisPool(config, "192.168.1.250", 7000, 0);
    }

  def getJedis: Jedis = {
     pool.getResource
  }

  def closeJedis(jedis: Jedis) {
    pool.returnResource(jedis)
  }

  def createJedisShardPool={
    val config: JedisPoolConfig = new JedisPoolConfig
    config.setMaxIdle(1000)
    config.setMaxWaitMillis(1000 * 10)
    config.setMinIdle(0)
    config.setTestOnBorrow(false)
    val jedisShardInfoList:util.ArrayList[JedisShardInfo]=new util.ArrayList[JedisShardInfo]()
    val jedisShardInfo: JedisShardInfo = new JedisShardInfo("192.168.1.250", 7000, "slave1")
    jedisShardInfo.setSoTimeout(1000 * 10)
    jedisShardInfoList.add(jedisShardInfo)
    val jedisShardInfo2: JedisShardInfo = new JedisShardInfo("192.168.1.250", 7001, "slave1")
    jedisShardInfo2.setSoTimeout(1000 * 10)
    jedisShardInfoList.add(jedisShardInfo2)
    val jedisShardInfo4: JedisShardInfo = new JedisShardInfo("192.168.1.250", 7002, "slave1")
    jedisShardInfo4.setSoTimeout(1000 * 10)
    jedisShardInfoList.add(jedisShardInfo4)
    val jedisShardInfo5: JedisShardInfo = new JedisShardInfo("192.168.1.250", 7003, "slave1")
    jedisShardInfo5.setSoTimeout(1000 * 10)
    jedisShardInfoList.add(jedisShardInfo5)
    val jedisShardInfo6: JedisShardInfo = new JedisShardInfo("192.168.1.250", 7004, "slave1")
    jedisShardInfo6.setSoTimeout(1000 * 10)
    jedisShardInfoList.add(jedisShardInfo6)

    shardPool = new ShardedJedisPool(config, jedisShardInfoList, Hashing.MD5)
    shardPool
  }

  def getShardJedis: ShardedJedis = {
    if(shardPool == null) shardPool=this.createJedisShardPool
     shardPool.getResource
  }

  def closeShardJedis(jedis: ShardedJedis) {
    shardPool.returnResource(jedis)
  }

  /**
   * jedisCluster
   * @return
   */
  def createJedisCluster={
    val jedisClusterNodes = new util.HashSet[HostAndPort]()
    jedisClusterNodes.add(new HostAndPort("192.168.1.120", 7000))
    jedisClusterNodes.add(new HostAndPort("192.168.1.120", 7001))
//    jedisClusterNodes.add(new HostAndPort("192.168.1.250", 7002))
//    jedisClusterNodes.add(new HostAndPort("192.168.1.250", 7003))
//    jedisClusterNodes.add(new HostAndPort("192.168.1.250", 7004))
//    jedisClusterNodes.add(new HostAndPort("192.168.1.250", 7005))
    new JedisCluster(jedisClusterNodes)

  }

  def main(args: Array[String]) {

    createJedisCluster

  }

  def getJedisCluster = {
    if(jedisCluster == null) jedisCluster=this.createJedisCluster else jedisCluster

//    jedisCluster.getClusterNodes
  }

}
