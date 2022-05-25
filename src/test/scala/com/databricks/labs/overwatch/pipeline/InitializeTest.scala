package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.PrivateMethodTester
import com.databricks.labs.overwatch.utils.Config


class InitializeTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper with PrivateMethodTester {

  describe("Tests for Initializer.isPVC") {
    it("should validate isPVC as false when org id if doesn't have ilb") {
      val conf = new Config
      conf.setOrganizationId("demo")
      val init = new Initializer(conf)

      val isPVC = init.getClass.getDeclaredMethod("isPVC")
      init.getClass.getDeclaredMethods.foreach(x=>println(x.getName))
      isPVC.setAccessible(true)
      val actual = isPVC.invoke(init)
      println(isPVC.invoke(init))

      assert(actual == false)
    }
  }

  describe("Tests for initialize database") {
    it("initializeDatabase function should create both elt and consumer database") {
      import spark.implicits._
      val conf = new Config
      conf.setDatabaseNameAndLoc("overwatch_etl", "/dblocation", "/datalocation")
      conf.setConsumerDatabaseNameandLoc("overwatch", "/consumer/dblocation")
      val init = new Initializer(conf)
      val database =  init.getClass.getDeclaredMethod("initializeDatabase")
      database.setAccessible(true)
      database.invoke(init)
      val databases = spark.sql("show databases").select("databaseName").map(f=>f.getString(0)).collect()
      databases.foreach(println)
      assert(databases.contains("overwatch_etl"))
      assert(databases.contains("overwatch"))
    }
  }
}