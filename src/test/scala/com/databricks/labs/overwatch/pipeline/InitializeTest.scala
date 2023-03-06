package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import com.databricks.labs.overwatch.env.Database
import com.databricks.labs.overwatch.utils.OverwatchScope._
import com.databricks.labs.overwatch.utils.{BadConfigException, Config, OverwatchScope}
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.io.JsonEOFException
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.{Ignore, PrivateMethodTester}
import org.scalatest.funspec.AnyFunSpec
@Ignore
class InitializeTest extends AnyFunSpec with DataFrameComparer with SparkSessionTestWrapper with PrivateMethodTester {
  describe("Tests for Initializer.isPVC") {
    it ("should validate isPVC as false when org id if doesn't have ilb") {
      val conf = new Config
      conf.setOrganizationId("demo")
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)

      val isPVC = PrivateMethod[Boolean]('isPVC)
      val actual = init.Init invokePrivate isPVC()
      println("value: " + actual)
      assert(!actual)
    }

    it("should validate isPVC as true when org id contains ilb") {
      val conf = new Config
      conf.setOrganizationId("demoilb")
      conf.setWorkspaceName("demoilbpvc")
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)

      val isPVC = PrivateMethod[Boolean]('isPVC)
      val actual = init.Init invokePrivate isPVC()
      println("value: " + actual)
      assert(actual)
    }
  }

  describe("Tests for initialize database") {

    it ("initializeDatabase function should create both elt and consumer database") {
      import spark.implicits._
      val conf = new Config
      conf.setDatabaseNameAndLoc("overwatch_etl", "file:/src/test/resources/overwatch/spark-warehouse/overwatch_etl.db", "file:/src/test/resources/overwatch/spark-warehouse/overwatch.db")
      conf.setConsumerDatabaseNameandLoc("overwatch", "file:/src/test/resources/overwatch/spark-warehouse/overwatch.db")
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val database = PrivateMethod[Database]('initializeDatabase)
      init.Init invokePrivate database()
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
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateIntelligentScaling = PrivateMethod[IntelligentScaling]('validateIntelligentScaling)

      assertThrows[BadConfigException](init.Init invokePrivate validateIntelligentScaling(intelligentScaling))
    }

    it("for intelligentScaling minimumCores can not be greater than maximum cores ") {
      import com.databricks.labs.overwatch.utils.IntelligentScaling

      val intelligentScaling = IntelligentScaling(enabled = true, 4, 1, 1.0)
      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateIntelligentScaling = PrivateMethod[IntelligentScaling]('validateIntelligentScaling)

      assertThrows[BadConfigException](init.Init invokePrivate validateIntelligentScaling(intelligentScaling))

    }

    it("for intelligentScaling coeff must be with in 0 to 10") {
      import com.databricks.labs.overwatch.utils.IntelligentScaling

      val intelligentScaling = IntelligentScaling(enabled = true, 4, 1, 12.0)
      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateIntelligentScaling = PrivateMethod[IntelligentScaling]('validateIntelligentScaling)

      assertThrows[BadConfigException](init.Init invokePrivate validateIntelligentScaling(intelligentScaling))
    }

    it("validateIntelligentScaling function should return IntelligentScaling case class upon correct validation") {
      import com.databricks.labs.overwatch.utils.IntelligentScaling

      val intelligentScaling = IntelligentScaling(enabled = true, 1, 10, 1.0)
      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateIntelligentScaling = PrivateMethod[IntelligentScaling]('validateIntelligentScaling)

      val actualIntelligentScaling = init.Init invokePrivate validateIntelligentScaling(intelligentScaling)

      assert(intelligentScaling == actualIntelligentScaling)
    }
  }

  describe("Tests for quickBuildAuditLogConfig configs") {

    it("quickBuildAuditLogConfig function should remove the last / from audit log path") {
      import com.databricks.labs.overwatch.utils.AuditLogConfig

      val configInput = AuditLogConfig(Some("path/to/auditLog/"), "Json", None)
      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val quickBuildAuditLogConfig = PrivateMethod[AuditLogConfig]('quickBuildAuditLogConfig)

      val expectedAuditConf = AuditLogConfig(Some("path/to/auditLog"), "json", None)
      val actualAuditConf = init.Init invokePrivate quickBuildAuditLogConfig(configInput)
      assert(expectedAuditConf == actualAuditConf)
    }

    it("quickBuildAuditLogConfig function build path from prefix for azure eventhub config") {
      import com.databricks.labs.overwatch.utils.{AuditLogConfig, AzureAuditLogEventhubConfig}

      val configInput = AuditLogConfig(None, "json", Some(AzureAuditLogEventhubConfig("sample.connection.string","auditLog",
        "path/to/auditLog/prefix/",10000,10, None, None)))

      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val quickBuildAuditLogConfig = PrivateMethod[AuditLogConfig]('quickBuildAuditLogConfig)

      val expectedAuditConf = AuditLogConfig(None, "json", Some(AzureAuditLogEventhubConfig("sample.connection.string",
        "auditLog", "path/to/auditLog/prefix",10000,10, Some("path/to/auditLog/prefix/rawEventsCheckpoint"), Some("path/to/auditLog/prefix/auditLogBronzeCheckpoint"))))
      val actualAuditConf = init.Init invokePrivate quickBuildAuditLogConfig(configInput)
      assert(expectedAuditConf == actualAuditConf)
    }


  }

  describe("Tests for validateAuditLogConfigs configs") {

    ignore ("validateAuditLogConfigs function validate auditLogPath in the config ") {
      import com.databricks.labs.overwatch.utils.AuditLogConfig

      val configInput = AuditLogConfig(None, "Json", None)
      val conf = new Config
      conf.setCloudProvider("aws")
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val quickBuildAuditLogConfig = PrivateMethod[AuditLogConfig]('validateAuditLogConfigs)

      assertThrows[BadConfigException](init.Init invokePrivate quickBuildAuditLogConfig(configInput))
    }

    ignore ("validateAuditLogConfigs function validate audit log format in the config ") {
      import com.databricks.labs.overwatch.utils.AuditLogConfig

      val configInput = AuditLogConfig(Some("path/to/audit/log"), "text", None)
      val conf = new Config
      conf.setCloudProvider("aws")
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val quickBuildAuditLogConfig = PrivateMethod[AuditLogConfig]('validateAuditLogConfigs)

      assertThrows[BadConfigException](init.Init invokePrivate quickBuildAuditLogConfig(configInput))
    }
  }


  describe("Tests for validateAndRegisterArgs function") {
    ignore ("validateAndRegisterArgs function should validate and register variables on the conf") {
      val incomplete = "{\"auditLogConfig\":{\"rawAuditPath\":\"/mnt/logs/test/audit_delivery\"}," +
        "\"tokenSecret\":{\"scope\":\"overwatch\",\"key\":\"1212\"},\"dataTarget\":{\"databaseName\":\"overwatch_etl_test\"," +
        "\"databaseLocation\":\"dbfs:/mnt/overwatch_global/consume/overwatch_etl.db\",\"consumerDatabaseName\":\"overwatch\"" +
        ",\"consumerDatabaseLocation\":\"dbfs:/mnt/overwatch_global/consume/overwatch.db\"}," +
        "\"badRecordsPath\":\"/mnt/overwatch/workspace-1029/sparkEventsBadrecords\",\"overwatchScope\":[\"all\"]," +
        "\"maxDaysToLoad\":60,\"databricksContractPrices\":{\"interactiveDBUCostUSD\":0.56,\"automatedDBUCostUSD\":0.26}," +
        "\"primordialDateString\":\"2021-01-16\"}"

      val conf = new Config
      conf.setOrganizationId("dummyorgifid")
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateAndRegisterArgs = PrivateMethod[Initializer]('validateAndRegisterArgs)
      init.Init invokePrivate validateAndRegisterArgs(incomplete)

      assert(conf.workspaceName == "dummyorgifid")
      assert(conf.databaseName == "overwatch_etl_test")
    }

    ignore ("validateAndRegisterArgs function should fail when a parameter is missing") {
      val incomplete = "{\"auditLogConfig\":{\"rawAuditPath\":\"/mnt/logs/test/audit_delivery\"}," +
        "\"dataTarget\":{" +
        "\"databaseLocation\":\"dbfs:/mnt/overwatch_global/consume/overwatch_etl.db\",\"consumerDatabaseName\":\"overwatch\"" +
        ",\"consumerDatabaseLocation\":\"dbfs:/mnt/overwatch_global/consume/overwatch.db\"}," +
        "\"badRecordsPath\":\"/mnt/overwatch/workspace-1029/sparkEventsBadrecords\",\"overwatchScope\":[\"all\"]," +
        "\"maxDaysToLoad\":60,\"databricksContractPrices\":{\"interactiveDBUCostUSD\":0.56,\"automatedDBUCostUSD\":0.26}}"

      val conf = new Config
      conf.setOrganizationId("dummyorgifid")
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateAndRegisterArgs = PrivateMethod[Initializer]('validateAndRegisterArgs)

      assertThrows[NoSuchElementException](init.Init invokePrivate validateAndRegisterArgs(incomplete))
    }

    it("validateAndRegisterArgs function should through exception when the json is malfunctioned ") {
      val incomplete = "{\"auditLogConfig\":{\"rawAuditPath\":\"/mnt/logs/test/audit_delivery\"," +
        "\"tokenSecret\":{\"scope\":\"overwatch\",\"key\":\"1212\"},\"dataTarget\":{\"databaseName\":\"overwatch_etl_test\"," +
        "\"databaseLocation\":\"dbfs:/mnt/overwatch_global/consume/overwatch_etl.db\",\"consumerDatabaseName\":\"overwatch\"" +
        ",\"consumerDatabaseLocation\":\"dbfs:/mnt/overwatch_global/consume/overwatch.db\"}," +
        "\"badRecordsPath\":\"/mnt/overwatch/workspace-1029/sparkEventsBadrecords\",\"overwatchScope\":[\"all\"]," +
        "\"maxDaysToLoad\":60,\"databricksContractPrices\":{\"interactiveDBUCostUSD\":0.56,\"automatedDBUCostUSD\":0.26}," +
        "\"primordialDateString\":\"2021-01-16\""

      val conf = new Config
      conf.setOrganizationId("dummyorgifid")
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateAndRegisterArgs = PrivateMethod[Initializer]('validateAndRegisterArgs)

      assertThrows[JsonEOFException](init invokePrivate validateAndRegisterArgs(incomplete))
    }

    it("validateAndRegisterArgs function should fail if json is non escaped") {
      val incomplete = "{'auditLogConfig':{'rawAuditPath':'/mnt/logs/test/audit_delivery'}," +
        "'tokenSecret':{'scope':'overwatch','key':'1212'},'dataTarget':{'databaseName':'overwatch_etl_test'," +
        "'databaseLocation':'dbfs:/mnt/overwatch_global/consume/overwatch_etl.db,consumerDatabaseName:overwatch'" +
        ",'consumerDatabaseLocation':'dbfs:/mnt/overwatch_global/consume/overwatch.db'}," +
        "'badRecordsPath':'/mnt/overwatch/workspace-1029/sparkEventsBadrecords','overwatchScope':['all']," +
        "'maxDaysToLoad':60,'databricksContractPrices':{'interactiveDBUCostUSD':0.56,'automatedDBUCostUSD':0.26}," +
        "'primordialDateString':'2021-01-16'}"
      val conf = new Config
      conf.setOrganizationId("dummyorgifid")
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateAndRegisterArgs = PrivateMethod[Initializer]('validateAndRegisterArgs)

      assertThrows[JsonParseException](init invokePrivate validateAndRegisterArgs(incomplete))
    }

  }

  describe("Tests for dataTargetIsValid function") {
    import com.databricks.labs.overwatch.utils.DataTarget
    it("dataTargetIsValid function should throw exception when the current db location is different than the one present already") {
      val dataTarget = DataTarget(Some("overwatch_etl"),Some("/path/to/database"), Some("/path/prefix"), Some("overwatch"), Some("/path/to/consumer_database"))
      spark.sql("create database if not exists overwatch_etl")

      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val dataTargetIsValid = PrivateMethod[Boolean]('dataTargetIsValid)
      assertThrows[BadConfigException](init.Init invokePrivate dataTargetIsValid(dataTarget))
    }

    ignore("dataTargetIsValid function should throw exception when the current db is not created from overwatch") {
      val dataTarget = DataTarget(Some("overwatch_etl"),Some("file:/src/test/resources/overwatch/spark-warehouse/overwatch_etl.db"), Some("/path/prefix"), Some("overwatch"), Some("/path/to/consumer_database"))
      spark.sql("create database if not exists overwatch_etl location 'file:/src/test/resources/overwatch/spark-warehouse/overwatch_etl.db'")
      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val dataTargetIsValid = PrivateMethod[Boolean]('dataTargetIsValid)
      assertThrows[BadConfigException](init.Init invokePrivate dataTargetIsValid(dataTarget))
    }
  }

  describe("Tests for validateScope function") {
    it("validateScope function should throw exception when the variable in scope is not one of the valid scope") {

      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateScope = PrivateMethod[Seq[OverwatchScope.OverwatchScope]]('validateScope)
      assertThrows[BadConfigException](init.Init invokePrivate validateScope(Seq("invalidScope")))
    }
    it("validateScope function should check if sparkEvents, clusterEvents, and jobs scopes require clusters scope") {

      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateScope = PrivateMethod[Seq[OverwatchScope.OverwatchScope]]('validateScope)
      assertThrows[IllegalArgumentException](init invokePrivate validateScope(Seq("jobs", "clusterEvents", "sparkEvents")))
    }

    it("validateScope function should return all scopes on validation") {
      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateScope = PrivateMethod[Seq[OverwatchScope.OverwatchScope]]('validateScope)
      val expectedScopeList = init.Init invokePrivate validateScope(Seq("audit", "notebooks", "accounts", "pools", "clusters", "clusterEvents", "sparkEvents", "jobs"))
      val actualScopeList = Seq(audit, notebooks, accounts, pools, clusters, clusterEvents, sparkEvents, jobs)
      assert(expectedScopeList == actualScopeList)
    }
    it("validateScope function should not be case sensitive") {
      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateScope = PrivateMethod[Seq[OverwatchScope.OverwatchScope]]('validateScope)
      val expectedScopeList = init.Init invokePrivate validateScope(Seq("Audit", "notebooks", "accounts", "pools", "CLUSTERS", "clusterEvents", "sparkEvents", "jobs"))
      val actualScopeList = Seq(audit, notebooks, accounts, pools, clusters, clusterEvents, sparkEvents, jobs)
      assert(expectedScopeList == actualScopeList)
    }
    it("validateScope function should work irrespective of order of scope") {
      val conf = new Config
      val init = new Initializer(conf, disableValidations = false, isSnap = false, initDB = true)
      val validateScope = PrivateMethod[Seq[OverwatchScope.OverwatchScope]]('validateScope)
      val expectedScopeList = init.Init invokePrivate validateScope(Seq("CLUSTERS", "clusterEvents", "sparkEvents", "jobs", "Audit", "notebooks", "accounts", "pools"))
      val actualScopeList = Seq(clusters, clusterEvents, sparkEvents, jobs, audit, notebooks, accounts, pools)
      assert(expectedScopeList == actualScopeList)
    }
  }

}