package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.Config
import org.apache.spark.sql.SparkSession

abstract class TestBronzeTransforms(_config: Config, spark: SparkSession)
  extends PipelineTargets(_config)
{


}
