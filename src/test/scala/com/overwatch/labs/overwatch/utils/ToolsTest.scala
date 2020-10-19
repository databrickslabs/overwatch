package com.overwatch.labs.overwatch.utils

import com.databricks.labs.overwatch.utils.JsonUtils
import com.overwatch.labs.overwatch.SparkSessionTestWrapper
import org.scalatest.FunSpec

class ToolSpec extends FunSpec with SparkSessionTestWrapper {

  describe("JsonUtils") {
    val jsonStr =
      """{"employees":[
        |    {"name":"Shyam", "email":"shyamjaiswal@gmail.com"},
        |    {"name":"Bob", "email":"bob32@gmail.com"},
        |    {"name":"Jai", "email":"jai87@gmail.com"}
        |]}  """.stripMargin

    it("should convert string to JSON") {
      val jsonMap = JsonUtils.jsonToMap(jsonStr)
      val bookArray = jsonMap.get("employees")
      assert(bookArray.isDefined && bookArray.get.isInstanceOf[List[Any]])
    }
  }

}
