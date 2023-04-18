## Fresh Overwatch Deployment to UC

UC Enabled overwatch job can be initiated in two different ways | DOCS.
For Overwatch UC Enablement no new configuration has been added. However the input format for the below configurations has been changed, apart from the format change of below configurations rest everything else is same | DOCS

ETL DatabaseName - <catalog_name>.<etl_database_name>

ConsumerDatabaseName - <catalog_name>.<consumer_database_name>

Storage Prefix -  <UC External Location>/<storage_prefix>

If the database is not provided in the mentioned format then default deployment will run and data will be stored to the hive_metastore instead of Unity Catalog.


## Migrating Existing Deployment From Hive_Metastore To UC

Config Changes - No new configuration has been added for UC Enablement. However the input format for the below configurations has been changed, apart from the format change of  the below configurations rest everything else is same| DOCS

ETL DatabaseName - <catalog_name>.<etl_database_name>

ConsumerDatabaseName - <catalog_name>.<consumer_database_name>

Storage Prefix -  <UC External Location>/<storage_prefix>


UC Enablement migration will happen in two stages -

1. Migrating existing Overwatch Data from hive_metastore to Unity Catalog
Migration will happen through a migration utility which will require inputs like -

   
Input Name | Input in Migration Utility Notebook | Description
:----------------------------|:------------------------------------|:--------------------------------------------------
Source ETL Database Name     | sourceEtlDBName                     |overwatch ETL Database Name in hive_metastore
Storage Prefix for UC        | storagePrefixForUC                  |new Storage Prefix for UC for Unity Catalog Deployment in this format - <UC External Location>/<new_ETL_DB_Name>
ETL Data Path for UC         | etlDataPathPrefixForUC              |new ETL Data Path for UC for Unity Catalog Deployment in this format - <UC_External_Location>/<new_ETL_DB_Name>/global_share
Catalog Name                 | catalogName                         |catalog name where new ETL lDB and Consumer DB will be stored
ETL Database Name for UC     | etlDB                               |new ETL Database Name for Unity Catalog Deployment. <catalog_Name>.<etl_DB_Name>
Consumer Database Name for UC| consumerDB                          |new Consumer Database Name for Unity Catalog Deployment. <catalog_Name>.<consumer_DB_Name>


After the migration all the tables in the ETL Database will be populated with the data from hive_metastore but views in the Consumer Database will be populated after the subsequent run of UC Enabled Overwatch Job  .
2. Resuming the Overwatch Job.

Step 1 can be ignored if you want to do a fresh deployment and do not want to carry forward the old data from hive_metastore to Unity Catalog. Fresh Deployment

### Migration Notebook 

```text
Migration Utility - hive_metastore to Unity Catalog
This notebook is used to migrate the existing overwatch data in hive_metastore to the Unity Catalog

Parameters
sourceEtlDBName = overwatch etlDatabase Name in hive_metastore

storagePrefixForUC = new storagePrefix for Unity Catalog Deployment in this format - <UC External Location>/<newETLDBName>

etlDataPathPrefixForUC = new etlDataPathPrefix for Unity Catalog Deployment in this format - <UC External Location>/<newETLDBName>/global_share

catalogName = catalog name where new etlDB and consumerDB will be stored

etlDB = new etlDatabase Name for Unity Catalog Deployment. <catalogName>.<etlDBName>

consumerDB = new consumerDatabase Name for Unity Catalog Deployment. <catalogName>.<consumerDBName>

Step for Validation
```

```text
// Run helper notebook
%run ../Helpers/_TestingHelpers_0720
```

```scala
// provide input to the variables
val sourceEtlDBName = "<sourceEtlDBName>"
val storagePrefixForUC = "<storagePrefixForUC>"
val etlDataPathPrefixForUC = s"${storagePrefixForUC}/global_share"
val catalogName = "<catalogName>"
val etlDB = s"${catalogName}.<etlDB>"
val consumerDB = s"${catalogName}.<consumerDB>"

// create workspace
val sourceWorkspace = Helpers.getWorkspaceByDatabase(s"${sourceEtlDBName}")
val b = Bronze(sourceWorkspace)
val s = Silver(sourceWorkspace)
val g = Gold(sourceWorkspace)

// get all the  bronze, silver and gold targets 
val targetsHiveMetastore = (b.getAllTargets ++ s.getAllTargets ++ g.getAllTargets :+ b.pipelineStateTarget).filter(_.exists(dataValidation = true))

// copy the data to the new etlDataPath Prefix
val targetsForUC = targetsHiveMetastore.map(t => {
  val newLocation = s"$etlDataPathPrefixForUC/${t.name}".toLowerCase
  val cloneDetail = CloneDetail(t.tableLocation, newLocation)
  cloneDetail
})
Helpers.parClone(targetsForUC)

// create target tables in the define Catalog 
val workspaceID = if (dbutils.notebook.getContext.tags("orgId") == "0") {
  dbutils.notebook.getContext.tags("browserHostName").split("\\.")(0)
} else dbutils.notebook.getContext.tags("orgId")
val dataTarget = Some(DataTarget(
  Some(etlDB), Some(s"${storagePrefixForUC}/${etlDB}.db"), Some(s"${storagePrefixForUC}/global_share"),
  Some(consumerDB), Some(s"${storagePrefixForUC}/${consumerDB}.db")
))
val badRecordsPath = Some(s"${storagePrefixForUC}/${workspaceID}/sparkEventsBadrecords")

// create new workspace config 
val targetWorkspaceOverrides = sourceWorkspace.getConfig.inputConfig.copy(dataTarget = dataTarget, badRecordsPath = badRecordsPath)

// generate new workspace
val targetWorkspaceOverrides = sourceWorkspace.getConfig.inputConfig.copy(dataTarget = dataTarget, badRecordsPath = badRecordsPath)

val b = Bronze(targetWorkspace)
val s = Silver(targetWorkspace)
val g = Gold(targetWorkspace)

// register target tables
(b.getAllTargets :+ b.pipelineStateTarget).foreach(table => 
  b.database.registerTarget(table))

s.getAllTargets.foreach(table=>
  s.database.registerTarget(table)
)

g.getAllTargets.foreach(table=>
  g.database.registerTarget(table)
)

// run Validation to check if all tables in UC are populated
def getRecordCountForAllTables(dbName: String): Unit = {
  val currentCatalog = spark.sessionState.catalogManager.currentCatalog.name
  val etlDB = if(dbName.contains(".")){
    spark.sessionState.catalogManager.setCurrentCatalog(dbName.split("\\.").head)
    dbName.split("\\.").last
  } else dbName
  spark.sessionState.catalog.listTables(etlDB).foreach{
    table=>println(table.table+" - "+spark.table(s"${etlDB}.${table.table}").count)
  }
  spark.sessionState.catalogManager.setCurrentCatalog(currentCatalog)
}

getRecordCountForAllTables("<catalog>.<etlDBName>")
```


