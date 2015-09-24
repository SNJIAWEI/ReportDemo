package donson.common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

/**
 * Created by Jiawei on 15/9/15.
 */
object SetApplicationLoggerLevel extends Logging{

  /**
   * 重写设置日志级别的方法
   * @param level
   */
  def setLevels(level : Level): Unit = {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(level)
    }
  }

}
