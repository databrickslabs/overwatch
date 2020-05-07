package com.databricks.labs.overwatch.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import org.apache.log4j.{Level, Logger}

trait SparkSessionWrapper extends Serializable {

  private val logger: Logger = Logger.getLogger(this.getClass)
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
      .enableHiveSupport()
      .config("spark.warehouse.dir", "metastore")
      .getOrCreate()
  }

  lazy val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN")
//  sc.setLogLevel("DEBUG")

  private var _coresPerWorker: Int = _
  private var _numberOfWorkerNodes: Int = _
  private var _totalCores: Int = _
  private var _coresPerTask: Int = _
  private var _parTasks: Int = _

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
  spark.conf.set("spark.sql.shuffle.partitions", getTotalCores * 4)
}
