package com.databricks.labs.overwatch.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * Enables access to the Spark variable.
 * Additional logic can be added to the if statement to enable different types of spark environments
 * Common uses include DBRemote and local, driver only spark, and local docker configured spark
 */
trait SparkSessionWrapper extends Serializable {

  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Init environment. This structure alows for multiple calls to "reinit" the environment. Important in the case of
   * autoscaling. When the cluster scales up/down envInit and then check for current cluster cores.
   */
  @transient
  lazy protected val _envInit: Boolean = envInit()

  /**
   * Access to spark
   * If testing locally or using DBConnect, the System variable "OVERWATCH" is set to "LOCAL" to make the code base
   * behavior differently to work in remote execution AND/OR local only mode but local only mode
   * requires some additional setup.
   */
  lazy val spark: SparkSession = if (System.getenv("OVERWATCH") != "LOCAL") {
    logger.log(Level.INFO, "Using Databricks SparkSession")
    SparkSession
      .builder().appName("OverwatchBatch")
      .getOrCreate()
  } else {
    logger.log(Level.INFO, "Using Custom, local SparkSession")
    SparkSession.builder()
      .master("local")
      .config("spark.driver.maxResultSize", "8g")
      .appName("OverwatchBatch")
//    Useful configs for local spark configs and/or using labs/spark-local-execution
//    https://github.com/databricks-academy/spark-local-execution
//      .config("spark.driver.bindAddress", "0.0.0.0")
//      .enableHiveSupport()
//      .config("spark.warehouse.dir", "metastore")
      .getOrCreate()
  }

  lazy val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("WARN")

  def getCoresPerWorker: Int = sc.parallelize("1", 1)
    .map(_ => java.lang.Runtime.getRuntime.availableProcessors).collect()(0)

  def getNumberOfWorkerNodes: Int = sc.statusTracker.getExecutorInfos.length - 1

  def getTotalCores: Int = getCoresPerWorker * getNumberOfWorkerNodes

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
}
