package com.databricks.labs.overwatch.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import org.apache.log4j.{Level, Logger}

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
      .appName("OverwatchBatch")
//      .config("spark.driver.bindAddress", "0.0.0.0")
//      .enableHiveSupport()
//      .config("spark.warehouse.dir", "metastore")
      .getOrCreate()
  }

  lazy val sc: SparkContext = spark.sparkContext
//  sc.setLogLevel("DEBUG")

  private var _coresPerWorker: Int = _
  private var _numberOfWorkerNodes: Int = _
  private var _totalCores: Int = _
  private var _coresPerTask: Int = _
  private var _parTasks: Int = _
  private val driverCores: Int = java.lang.Runtime.getRuntime.availableProcessors

  def setCoresPerWorker(value: Int): this.type = {
    _coresPerWorker = value
    this
  }

  def setNumberOfWorkerNodes(value: Int): this.type = {
    _numberOfWorkerNodes = value
    this
  }

  def setTotalCores(value: Int): this.type = {
    _totalCores = value
    this
  }

  def setCoresPerTask(value: Int): this.type = {
    _coresPerTask = value
    this
  }

  def setParTasks(value: Int): this.type = {
    _parTasks = value
    this
  }

  def getCoresPerWorker: Int = _coresPerWorker

  def getNumberOfWorkerNodes: Int = _numberOfWorkerNodes

  def getTotalCores: Int = _totalCores

  def getCoresPerTask: Int = _coresPerTask

  def getParTasks: Int = _parTasks

  def getDriverCores: Int = driverCores

  /**
   * Set global, cluster details such as cluster cores, driver cores, logLevel, etc.
   * This also provides a simple way to change the logging level throuhgout the package
   * @param logLevel log4j log level
   * @return
   */
  def envInit(logLevel: String = "INFO"): Boolean = {
    sc.setLogLevel(logLevel)
    if (System.getenv("OVERWATCH") != "LOCAL") {
      setCoresPerWorker(sc.parallelize("1", 1)
        .map(_ => java.lang.Runtime.getRuntime.availableProcessors).collect()(0))
      
      setNumberOfWorkerNodes(sc.statusTracker.getExecutorInfos.length - 1)
    } else {
      val env = System.getenv().asScala
      setCoresPerWorker(env("coresPerWorker").toInt)
      setNumberOfWorkerNodes(env("numberOfWorkerNodes").toInt)
    }
    setTotalCores(getCoresPerWorker * getNumberOfWorkerNodes)
    setCoresPerTask(
      try {
        spark.conf.get("spark.task.cpus").toInt
      }
      catch {
        case e: java.util.NoSuchElementException => 1
      }
    )
    setParTasks(scala.math.floor(getTotalCores / getCoresPerTask).toInt)
//    if (spark.conf.get("spark.sql.shuffle.partitions") == "200")
//      spark.conf.set("spark.sql.shuffle.partitions", getTotalCores * 4)

    true
  }
}
