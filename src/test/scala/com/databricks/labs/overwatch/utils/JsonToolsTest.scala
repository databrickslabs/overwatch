package com.databricks.labs.overwatch.utils

import org.scalatest.funspec.AnyFunSpec

import scala.collection.JavaConverters._

class JsonToolsTest extends AnyFunSpec {

  describe("JsonUtils") {
    case class TestData(id: String, b: Boolean, l: Seq[String], opt: Option[String], m: Map[String, String])

    val commonNoData: TestData = TestData(null, false, Seq(), None, Map())
    it("should convert JSON string to Map") {
      val jsonStr =
        """{"employees":[
          |    {"name":"Shyam", "email":"shyamjaiswal@gmail.com"},
          |    {"name":"Bob", "email":"bob32@gmail.com"},
          |    {"name":"Jai", "email":"jai87@gmail.com"}
          |]}  """.stripMargin

      val jsonMap = JsonUtils.jsonToMap(jsonStr)
      val bookArray = jsonMap.get("employees")
      assert(bookArray.isDefined && bookArray.get.isInstanceOf[List[Any]])
    }

    it("shouldn't convert JSON string to Map") {
      val jsonStr = """{"employees":""".stripMargin
      val jsonMap = JsonUtils.jsonToMap(jsonStr)
      assertResult(1)(jsonMap.size)
      assertResult(true)(jsonMap.contains("ERROR"))
    }

    it("Issue 105 is fixed") {
      val jsonStr = scala.io.Source.fromFile("src/test/resources/overwatch-json-error.json").mkString
      val jsonMap = JsonUtils.jsonToMap(jsonStr)
      assertResult(1)(jsonMap.size)
      assertResult(true)(jsonMap.contains("jobs"))
    }

    it("should convert almost empty object to JSON string without nulls & empty values") {
      val js = JsonUtils.objToJson(commonNoData)
      assertResult(commonNoData)(js.fromObj)
      assertResult("{\"b\":false}")(js.compactString)
      assertResult("{\\\"b\\\":false}")(js.escapedString)
      assertResult(
        """{
          |  "b" : false
          |}""".stripMargin)(js.prettyString)
    }

    it("should convert non-empty object to JSON string without nulls & empty values") {
      val commonData = TestData("test", true, Seq("1"), Some("2"), Map("3" -> "4"))

      val js = JsonUtils.objToJson(commonData)
      assertResult(commonData)(js.fromObj)
      assertResult(
        "{\"id\":\"test\",\"b\":true,\"l\":[\"1\"],\"opt\":\"2\",\"m\":{\"3\":\"4\"}}"
      )(js.compactString)
      assertResult(
        "{\\\"id\\\":\\\"test\\\",\\\"b\\\":true,\\\"l\\\":[\\\"1\\\"],\\\"opt\\\":\\\"2\\\",\\\"m\\\":{\\\"3\\\":\\\"4\\\"}}"
      )(js.escapedString)
      assertResult(
        """{
          |  "id" : "test",
          |  "b" : true,
          |  "l" : [ "1" ],
          |  "opt" : "2",
          |  "m" : {
          |    "3" : "4"
          |  }
          |}""".stripMargin)(js.prettyString)
    }

    // TODO: debug why 'includeNulls = false' is ignored...
    /*it("should convert almost empty object to JSON string without nulls & with empty values") {
      val js = JsonUtils.objToJson(commonNoData, includeEmpty = true)
      assertResult(commonNoData)(js.fromObj)
      assertResult("{\"b\":false,\"l\":[],\"m\":{}}")(js.compactString)
      assertResult("{\\\"b\\\":false,\\\"l\\\":[],\\\"m\\\":{}}")(js.escapedString)
      assertResult(
        """{
          |  "b" : false,
          |  "l" : [ ],
          |  "m" : { }
          |}""".stripMargin)(js.prettyString)
    }*/

    it("should convert almost empty object to JSON string with nulls & with empty values") {
      val js = JsonUtils.objToJson(commonNoData, includeEmpty = true, includeNulls = true)
      assertResult(commonNoData)(js.fromObj)
      //      println(s"${js.compactString}")
      //      println(s"${js.escapedString}")
      //      println(s"${js.prettyString}")
      assertResult("{\"id\":null,\"b\":false,\"l\":[],\"opt\":null,\"m\":{}}")(js.compactString)
      assertResult("{\\\"id\\\":null,\\\"b\\\":false,\\\"l\\\":[],\\\"opt\\\":null,\\\"m\\\":{}}")(js.escapedString)
      assertResult(
        """{
          |  "id" : null,
          |  "b" : false,
          |  "l" : [ ],
          |  "opt" : null,
          |  "m" : { }
          |}""".stripMargin)(js.prettyString)
    }

    it("should convert Java map to JSON string without nulls & with empty values") {
      val javaMap = Map("sfield1" -> "value", "sfield2" -> "", "sfield3" -> null,
        "ifield" -> 0, "bfield" -> true).asJava
      val js = JsonUtils.objToJson(javaMap, includeEmpty = true)
      assertResult(javaMap)(js.fromObj)
      assertResult(
        "{\"ifield\":0,\"sfield2\":\"\",\"sfield1\":\"value\",\"bfield\":true}"
      )(js.compactString)
      assertResult(
        "{\\\"ifield\\\":0,\\\"sfield2\\\":\\\"\\\",\\\"sfield1\\\":\\\"value\\\",\\\"bfield\\\":true}"
      )(js.escapedString)
      assertResult(
        """{
          |  "ifield" : 0,
          |  "sfield2" : "",
          |  "sfield1" : "value",
          |  "bfield" : true
          |}""".stripMargin)(js.prettyString)
    }

    it("should convert Scala map to JSON string without nulls & with empty values") {
      val scalaMap = Map[String, Any]("sfield1" -> "value", "sfield2" -> "",
        "sfield3" -> null, "ifield" -> 0, "bfield" -> true)
      val js = JsonUtils.objToJson(scalaMap, includeEmpty = true)
      assertResult(scalaMap)(js.fromObj)
      assertResult(
        "{\"ifield\":0,\"sfield2\":\"\",\"sfield1\":\"value\",\"bfield\":true}"
      )(js.compactString)
      assertResult(
        "{\\\"ifield\\\":0,\\\"sfield2\\\":\\\"\\\",\\\"sfield1\\\":\\\"value\\\",\\\"bfield\\\":true}"
      )(js.escapedString)
      assertResult(
        """{
          |  "ifield" : 0,
          |  "sfield2" : "",
          |  "sfield1" : "value",
          |  "bfield" : true
          |}""".stripMargin)(js.prettyString)
    }

    it("should convert empty Scala map to JSON string without nulls & with empty values") {
      val scalaMap = Map[String, Any]()
      val js = JsonUtils.objToJson(scalaMap)
      assertResult(scalaMap)(js.fromObj)
      assertResult(
        "{}"
      )(js.compactString)
      assertResult(
        "{}"
      )(js.escapedString)
      assertResult(
        "{ }".stripMargin)(js.prettyString)
    }

  }
}
