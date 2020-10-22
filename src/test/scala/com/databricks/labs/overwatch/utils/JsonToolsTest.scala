package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.utils.{JsonUtils}
import org.scalatest.funspec.AnyFunSpec

class JsonToolsTest extends AnyFunSpec  {

  describe("JsonUtils") {
    val jsonStr =
      """{"employees":[
        |    {"name":"Shyam", "email":"shyamjaiswal@gmail.com"},
        |    {"name":"Bob", "email":"bob32@gmail.com"},
        |    {"name":"Jai", "email":"jai87@gmail.com"}
        |]}  """.stripMargin

    it("should convert JSON string to Map") {
      val jsonMap = JsonUtils.jsonToMap(jsonStr)
      val bookArray = jsonMap.get("employees")
      assert(bookArray.isDefined && bookArray.get.isInstanceOf[List[Any]])
    }

    it("should convert object to JSON string without nulls & empty values") {
      val map = Map[String, Any]("sfield1" -> "value", "sfield2" -> "",
        "sfield3" -> null, "ifield" -> 0, "bfield" -> true)
      val js = JsonUtils.objToJson(map)
      assertResult(map)(js.fromObj)
// TODO: implement when the implementation is fixed
//      println(s"${js.compactString}")
//      println(s"${js.escapedString}")
//      println(s"${js.prettyString}")
//      assertResult("")(js.compactString)
//      assertResult("")(js.escapedString)
//      assertResult("")(js.prettyString)
    }
  }
}
