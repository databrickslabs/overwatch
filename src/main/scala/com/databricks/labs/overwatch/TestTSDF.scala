package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.overwatch.pipeline.TransformFunctions._

object TestTSDF extends SparkSessionWrapper{

//  def main(args: Array[String]): Unit = {
//    import spark.implicits._
//
//    val left = Seq(
//      ("g1", 10, 2, Some(0), None),
//      ("g1", 11, 2, Some(1), None),
//      ("g1", 12, 3, None, None),
//      ("g1", 13, 3, Some(6), None),
//      ("g2", 9, 3, Some(12), None),
//      ("g2", 12, 3, Some(4), Some(8)),
//      ("g2", 17, 3, Some(7), None),
//      ("g2", 20, 3, Some(3), None),
//      ("g3", 11, 3, Some(7), Some(4)),
//      ("g3", 17, 3, None, None),
//      ("g3", 19, 3, None, None)
//    ).toDF("key", "ts", "z", "v1", "v2")
//
//    val right = Seq(
//      ("g1", 9, Some(4), Some(42)),
//      ("g2", 15, Some(12), None),
//      ("g2", 16, Some(32), Some(98)),
//      ("g2", 22, Some(34), Some(12)),
//      ("g3", 11, Some(7), Some(4)),
//      ("g3", 26, None, Some(33)),
//      ("g3", 28, Some(44), Some(52))
//    ).toDF("key", "ts", "v1", "v2")
//
//    val outDF = left.toTSDF("ts", "key")
//      .lookupWhen(
//        right.toTSDF("ts", "key"),
//        tsPartitionVal = 4
//      ).df
//
//    outDF
//      .select("key", "ts", "v1", "v2")
//      .orderBy('key, 'ts).show(20, false)
//  }
}
