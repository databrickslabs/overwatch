package com.databricks.labs.overwatch.utils

import org.scalatest.funspec.AnyFunSpec

class ToolTest extends AnyFunSpec {

  describe("Helpers Test") {
    it("Numeric test"){
      assert(Helpers.isNumeric("1234") == true)
      assert(Helpers.isNumeric("abcd") == false)
    }

  }

}
