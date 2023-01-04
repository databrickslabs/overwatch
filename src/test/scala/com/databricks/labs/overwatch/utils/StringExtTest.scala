package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.utils.StringExt.StringHelpers
import org.scalatest.funspec.AnyFunSpec

class StringExtTest extends AnyFunSpec {

  describe("StringHelpers Test") {
    it("Simple String Test No Special Characters") {
      assert("abc".containsNoSpecialChars)
    }

    it("String with only _") {
      assert("abc_xyz".containsNoSpecialChars)
    }

    it("String with Period") {
      assert(!"abc.xyz".containsNoSpecialChars)
    }

    it("String with special characters") {
      assert(!"abc_\\\\xyz".containsNoSpecialChars)
    }

    it("String with white spaces characters") {
      assert("Business Name".containsNoSpecialChars)
    }

    it("String with white % characters") {
      assert(!"""Business %Name""".containsNoSpecialChars)
    }

    it("String with white $ characters") {
      assert(!"""Business $Name""".containsNoSpecialChars)
    }
  }
}
