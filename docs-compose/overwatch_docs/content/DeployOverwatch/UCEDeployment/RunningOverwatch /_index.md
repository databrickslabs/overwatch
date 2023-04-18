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