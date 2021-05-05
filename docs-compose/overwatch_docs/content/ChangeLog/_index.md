---
title: "ChangeLog"
date: 2021-05-05T17:00:13-04:00
weight: 4
---

## 0.4.11
* Hotfix for [Issue 119](https://github.com/databrickslabs/overwatch/issues/119). Issue was only present in edge cases.
  Edge cases include workspaces missing data and attempting to run modules for which data didn't exist.
* Added additional guard rails to pipeline state checks to help clarify workspace / pipeline state issues for
  future users.

## 0.4.1
* [Converted all delta targets from managed tables to external tables](https://github.com/databrickslabs/overwatch/issues/50)
    * Better security and best practice
    * Enabled etl data path prefix to simplify and protect the raw data across workspaces
* pipeline_report - enabled partitioning by "organization_id"
    * Necessary to guard against write concurrency issues across multiple workspaces
* orgianization_id - Single Tenant AWS workspaces with DNS would return "0" for organization_id, updated to change
  organization_id to the workspace prefix.
    * EXAMPLE: https://myorg_dev.cloud.databricks.com --> myorg_dev
* instanceDetails -
    * Converted from append mode to overwrite mode. Allows for multiple workspaces to create the compute pricing lookup
      if it didn't already exist for the workspace
    * Moved default table location from consumerDB to ETLDB and created mapped view to table in consumerDB
    * Simplified the instanceDetails cost customization process (docs updated)
    * Added proper filter to ensure ONLY current workspace compute costs are looked up during joins
    * Converted Azure example Memory_GB to double to match data type from AWS
* Bug fix for JSON to Map that caused certain escaped json strings to break when being pulled from the new Jobs UI
* Bug fix for [Issue 111](https://github.com/databrickslabs/overwatch/issues/111)
* Bug fix for multi-workspace Azure, filter from audit_raw_land table was incomplete
* Bug fix to allow for consumerDB to be omitted from DataTarget in config

## 0.4.0
Initial Public Release