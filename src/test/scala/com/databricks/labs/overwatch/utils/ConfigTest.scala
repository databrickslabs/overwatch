package com.databricks.labs.overwatch.utils

import org.scalatest.funspec.AnyFunSpec

class ConfigTest extends AnyFunSpec {

  describe("ConfigTest") {
    it("should createTimeDetail") {

      val tsMilli = 1234567890123L
      val tmStruct = Config.createTimeDetail(tsMilli)

      assertResult(tsMilli)(tmStruct.asUnixTimeMilli)
      assertResult(tsMilli/1000)(tmStruct.asUnixTimeS)
      assert(tmStruct.asDTString.startsWith("2009-02-1"))
      assert(tmStruct.asTSString.contains(":31:30"))
    }
  }
}
