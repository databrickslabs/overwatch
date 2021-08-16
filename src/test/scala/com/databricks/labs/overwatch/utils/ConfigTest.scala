package com.databricks.labs.overwatch.utils

import org.scalatest.funspec.AnyFunSpec

class ConfigTest extends AnyFunSpec {

//  describe("Config Tests") {
//
//    val config = new Config()
//    val managedSourceURI = "dbfs:/user/hive/warehouse/overwatch"
//    val managedSourceURIWSpecialChar = "dbfs:/user/hive/warehouse/overwatch.db"
//    val managedSourcePath = "/user/hive/warehouse/overwatch"
//    val unmanagedSourceURI = "s3a:/mnt/overwatch/overwatch_globaldb"
//    val unmanagedSourceURIWSpecialChar = "s3a:/mnt/overwatch/overwatch_global.db"
//    val unmanagedSourcePath = "/mnt/overwatch/overwatch_globaldb"
//
//    it("should be equal to the original dbLocation String") {
//      val deriveDataPrefix = PrivateMethod[String]('deriveDataPrefix)
//      val v1 = config invokePrivate deriveDataPrefix(managedSourceURI)
//      val v2 = config invokePrivate deriveDataPrefix(managedSourceURIWSpecialChar)
//      val v3 = config invokePrivate deriveDataPrefix(managedSourcePath)
//
//      assert(v1 == managedSourceURI)
//      assert(v2 == managedSourceURIWSpecialChar)
//      assert(v3 == managedSourcePath)
//    }
//
//    it("should be the original string with the suffix _data"){
//      val deriveDataPrefix = PrivateMethod[String]('deriveDataPrefix)
//      val v1 = config invokePrivate deriveDataPrefix(unmanagedSourceURI)
//      val v2 = config invokePrivate deriveDataPrefix(unmanagedSourceURIWSpecialChar)
//      val v3 = config invokePrivate deriveDataPrefix(unmanagedSourcePath)
//      assert(v1 == s"${unmanagedSourceURI}_data")
//      assert(v2 == s"${unmanagedSourceURIWSpecialChar}_data")
//      assert(v3 == s"${unmanagedSourcePath}_data")
//    }
//
//  }

//  describe("ConfigTest") {
//    it("should createTimeDetail") {
//
//      val tsMilli = 1234567890123L
//      val tmStruct = Config.createTimeDetail(tsMilli)
//
//      assertResult(tsMilli)(tmStruct.asUnixTimeMilli)
//      assertResult(tsMilli/1000)(tmStruct.asUnixTimeS)
//      assert(tmStruct.asDTString.startsWith("2009-02-1"))
//      assert(tmStruct.asTSString.contains(":31:30"))
//    }
//  }
}
