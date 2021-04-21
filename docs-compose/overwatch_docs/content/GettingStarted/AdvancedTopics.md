---
title: "Advanced Topics"
date: 2020-10-28T13:43:52-04:00
weight: 4
---

* Multiple Workspaces
* Security / Views
    * Recommendations for Token (Ownership)
    * Required Privileges / Access for Token
    * Job owner must have access to the secret
* Directory Lifecycle (dropping raw log files)
    * SC-38627 filed to improve raw log delivery
* Moving the Overwatch Database
* Getting through the historical load (first run)

## Joining With Slow Changing Dimensions (SCD)
The nature of time throughout the Overwatch project has resulted in the need to for 
advanced time-series DataFrame management. As such, a vanilla version of 
[databrickslabs/tempo](https://github.com/databrickslabs/tempo) was implemented 
for the sole purpose of enabling Scala time-series DataFrames (TSDF). TSDFs enable
"asofJoin" and "lookupWhen" functions that also efficiently handle massive skew as is 
introduced with the partitioning of organization_id and cluster_id. Please refer to the Tempo
documentation for deeper info. When Tempo's implementation for Scala is complete, Overwatch plans to 
simply reference it as a dependency.

This is discussed here because these functionalities have been made public through Overwatch which 
means you can easily utilize "lookupWhen" and "asofJoin" when interrogating Overwatch. The details for 
optimizing skewed windows is beyond the scope of this documentation but please do reference the Tempo 
documentation for more details.

In the example below, assume you wanted the cluster name at the time of some event in a fact table.
Since cluster names can be edited, this name could be different throughout time so what was that 
value at the time of some event in a driving table.

The function signature for "lookupWhen" is:
```scala
  def lookupWhen(
                  rightTSDF: TSDF,
                  leftPrefix: String = "",
                  rightPrefix: String = "right_",
                  maxLookback: Long = Window.unboundedPreceding,
                  maxLookAhead: Long = Window.currentRow,
                  tsPartitionVal: Int = 0,
                  fraction: Double = 0.1
                ): TSDF = ???
```

```scala
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
val metricDf = Seq(("cluster_id", 10, 1609459200000L)).toDF("partCol", "metric", "timestamp")
val lookupDf = Seq(
  ("0218-060606-rouge895", "my_clusters_first_name", 1609459220320L),
  ("0218-060606-rouge895", "my_clusters_new_name", 1609458708728L)
).toDF("cluster_id", "cluster_name", "timestamp")

metricDf.toTSDF("timestamp", "cluster_id")
  .lookupWhen(
    lookupDf.toTSDF("timestamp", "cluster_id")
  )
  .df
  .show(20, false)
```



## Optimizations Implemented

## Best Practices

## Enabling Debug
Might move this to troubleshooting