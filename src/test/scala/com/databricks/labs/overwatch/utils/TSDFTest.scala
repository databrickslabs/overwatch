package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.SparkSessionTestWrapper
import org.scalatest.GivenWhenThen
import org.scalatest.funspec.AnyFunSpec
import com.databricks.labs.overwatch.pipeline.TransformFunctions._

class TSDFTest extends AnyFunSpec with SparkSessionTestWrapper with GivenWhenThen {
  import spark.implicits._

  private val lookupDF = Seq(
    (2, 123, "testJob"),
    (4, 123, "testJobUpdated"),
    (6, 123, "testJobUpdatedAgain"),
    (1, 124, "anotherJobName"),
  ).toDF("timestamp", "jobId", "jobName")

  private val drivingDF = Seq(
    (3, 123),
    (4, 123),
    (5, 123),
    (7, 123),
    (5, 124),
    (6, 125),
  ).toDF("timestamp", "jobId")

  describe("lookupWhen") {

    it ("should complete a TSDF lookupWhen") {

      val outDF = drivingDF.toTSDF("timestamp", "jobId")
        .lookupWhen(lookupDF.toTSDF("timestamp", "jobId"))
        .df

      val expectedDF = Seq(
        (3, 123, "testJob"),
        (4, 123, "testJob"),
        (5, 123, "testJobUpdated"),
        (7, 123, "testJobUpdatedAgain"),
        (5, 124, "anotherJobName"),
        (6, 125, null)
      ).toDF("timestamp", "jobId", "jobName")

      assertResult(expectedDF.collect())(outDF.collect())

    }

  }

}
