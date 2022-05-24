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
      val isPVC =PrivateMethod[Initializer]('isPVC)
      Initializer invokePrivate isPVC()
    }

    /*it("should validate isPVC as true when org id if have ilb") {
      val conf = new Config
      conf.setOrganizationId("demoilb")
      conf.setWorkspaceName("demo")
      val init = new Initializer(conf)
      assert(init.isPVC === true)
    }*/
  }
}