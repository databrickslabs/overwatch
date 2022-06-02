package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.PrivateMethodTester
import com.databricks.labs.overwatch.utils.{BadConfigException, Config}
import com.databricks.labs.overwatch.env.Database

class InitializeTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper with PrivateMethodTester {

  describe("Tests for Initializer.isPVC") {
    it("should validate isPVC as false when org id if doesn't have ilb") {
      val conf = new Config
      conf.setOrganizationId("demo")
      val init = new Initializer(conf)

      val isPVC = PrivateMethod[Boolean]('isPVC)
      val actual = init invokePrivate isPVC()
      println("value: " + actual)
      assert(!actual)
    }
  }

  describe("Tests for initialize database") {
    it("initializeDatabase function should create both elt and consumer database") {
      import spark.implicits._
      val conf = new Config
      conf.setDatabaseNameAndLoc("overwatch_etl", "/dblocation", "/datalocation")
      conf.setConsumerDatabaseNameandLoc("overwatch", "/consumer/dblocation")
      val init = new Initializer(conf)
      val database = PrivateMethod[Database]('initializeDatabase)
      init invokePrivate database()
      val databases = spark.sql("show databases").select("namespace").map(f => f.getString(0)).collect()
      assert(databases.contains("overwatch_etl"))
      assert(databases.contains("overwatch"))
    }
  }

  describe("Tests for validateIntelligentScaling configs") {
    it("for intelligentScaling minimumCores should not be less than 1 ") {
      import com.databricks.labs.overwatch.utils.IntelligentScaling

      val intelligentScaling = IntelligentScaling(enabled = true, 0, 123, 1.0)
      val conf = new Config
      val init = new Initializer(conf)
      val validateIntelligentScaling = PrivateMethod[IntelligentScaling]('validateIntelligentScaling)

      assertThrows[BadConfigException](init invokePrivate validateIntelligentScaling(intelligentScaling))
    }

    it("for intelligentScaling minimumCores can not be greater than maximum cores ") {
      import com.databricks.labs.overwatch.utils.IntelligentScaling

      val intelligentScaling = IntelligentScaling(enabled = true, 4, 1, 1.0)
      val conf = new Config
      val init = new Initializer(conf)
      val validateIntelligentScaling = PrivateMethod[IntelligentScaling]('validateIntelligentScaling)

      assertThrows[BadConfigException](init invokePrivate validateIntelligentScaling(intelligentScaling))

    }

    it("for intelligentScaling coeff must be with in 0 to 10") {
      import com.databricks.labs.overwatch.utils.IntelligentScaling

      val intelligentScaling = IntelligentScaling(enabled = true, 4, 1, 12.0)
      val conf = new Config
      val init = new Initializer(conf)
      val validateIntelligentScaling = PrivateMethod[IntelligentScaling]('validateIntelligentScaling)

      assertThrows[BadConfigException](init invokePrivate validateIntelligentScaling(intelligentScaling))
    }

    it("validateIntelligentScaling function should return IntelligentScaling case class upon correct validation") {
      import com.databricks.labs.overwatch.utils.IntelligentScaling

      val intelligentScaling = IntelligentScaling(enabled = true, 1, 10, 1.0)
      val conf = new Config
      val init = new Initializer(conf)
      val validateIntelligentScaling = PrivateMethod[IntelligentScaling]('validateIntelligentScaling)

      val actualIntelligentScaling = init invokePrivate validateIntelligentScaling(intelligentScaling)

      assert(intelligentScaling == actualIntelligentScaling)
    }

  }
}

