package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.utils.SparkSessionWrapper.parSessionsOn
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.eclipse.jetty.util.ConcurrentHashSet

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._


object SparkSessionWrapper {

  var parSessionsOn = false
  private[overwatch] val sessionsMap = new ConcurrentHashMap[Long, SparkSession]().asScala
  private[overwatch] val globalTableLock = new ConcurrentHashSet[String]
  private[overwatch] val globalSparkConfOverrides = Map(
    "spark.sql.shuffle.partitions" -> "800", // allow aqe to shrink
    "spark.databricks.adaptive.autoOptimizeShuffle.enabled" -> "false", // disabled -- perf issue 794
    "spark.databricks.delta.optimize.maxFileSize" -> "134217728", // 128 MB default
    "spark.sql.files.maxPartitionBytes" -> "134217728", // 128 MB default
    "spark.sql.caseSensitive" -> "false",
    "spark.sql.autoBroadcastJoinThreshold" -> "10485760",
    "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "10485760",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact" -> "false",
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite" -> "false",
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "true",
    "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "50000",
    "spark.databricks.delta.optimizeWrite.binSize" -> "512",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "268435456", // reset to default 256MB
    "spark.sql.optimizer.collapseProjectAlwaysInline" -> "true" // temporary workaround ES-318365
  )

}

/**
 * Enables access to the Spark variable.
 * Additional logic can be added to the if statement to enable different types of spark environments
 * Common uses include DBRemote and local, driver only spark, and local docker configured spark
 */
trait SparkSessionWrapper extends Serializable {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val sessionsMap = SparkSessionWrapper.sessionsMap

  /**
   * Init environment. This structure alows for multiple calls to "reinit" the environment. Important in the case of
   * autoscaling. When the cluster scales up/down envInit and then check for current cluster cores.
   */
  lazy protected val _envInit: Boolean = envInit()


  private def buildSpark(): SparkSession = {
    SparkSession
      .builder()
      .appName("Overwatch - GlobalSession")
      .getOrCreate()
  }
  /**
   * Access to spark
   * If testing locally or using DBConnect, the System variable "OVERWATCH" is set to "LOCAL" to make the code base
   * behavior differently to work in remote execution AND/OR local only mode but local only mode
   * requires some additional setup.
   */
  private[overwatch] def spark(globalSession : Boolean = false): SparkSession = {

    if(SparkSessionWrapper.parSessionsOn){
      if(globalSession){
        buildSpark()
      }
      else{
        val currentThreadID = Thread.currentThread().getId
        val sparkSession = sessionsMap.getOrElse(currentThreadID, buildSpark().newSession())
        sessionsMap.put(currentThreadID, sparkSession)
        sparkSession
      }
    }else{
      buildSpark()
    }
  }

  @transient lazy val spark:SparkSession = spark(false)

  lazy val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("WARN")

  protected def clearThreadFromSessionsMap(): Unit ={
    sessionsMap.remove(Thread.currentThread().getId)
    logger.log(Level.INFO, s"""Removed ${Thread.currentThread().getId} from sessionMap""")
  }

  def getCoresPerWorker: Int = sc.parallelize("1", 1)
    .map(_ => java.lang.Runtime.getRuntime.availableProcessors).collect()(0)

  def getNumberOfWorkerNodes: Int = sc.statusTracker.getExecutorInfos.length - 1

  def getTotalCores: Int = {
    val totalWorkCores = getCoresPerWorker * getNumberOfWorkerNodes
    // handle for single node clusters
    if (totalWorkCores == 0) getDriverCores else totalWorkCores
  }

  def getCoresPerTask: Int = {
    try {
      spark.conf.get("spark.task.cpus").toInt
    }
    catch {
      case _: java.util.NoSuchElementException => 1
    }
  }

  def getParTasks: Int = scala.math.floor(getTotalCores / getCoresPerTask).toInt

  def getDriverCores: Int = java.lang.Runtime.getRuntime.availableProcessors

  /**
   * Set global, cluster details such as cluster cores, driver cores, logLevel, etc.
   * This also provides a simple way to change the logging level throuhgout the package
   * @param logLevel log4j log level
   * @return
   */
  def envInit(logLevel: String = "INFO"): Boolean = {
    if (System.getenv().containsKey("LOGLEVEL")) sc.setLogLevel(System.getenv("LOGLEVEL"))
    else sc.setLogLevel(logLevel)
    true
  }

  /**
   * This function set the current catalog Name for the entire spark session
   * @param catalogName
   */
  def setCurrentCatalog(spark: SparkSession, catalogName: String): Unit = {
    spark.sessionState.catalogManager.setCurrentCatalog(catalogName)
  }

  /**
     * This function fetches the current catalog name if the spark session
   */
    def getCurrentCatalogName(spark: SparkSession): String = {
      spark.sessionState.catalogManager.currentCatalog.name()
    }

}
