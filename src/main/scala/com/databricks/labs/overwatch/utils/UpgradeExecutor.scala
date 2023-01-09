package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.env.Workspace
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class UpgradeExecutor(val etldbname: String, val fromVersion: Int, val toVersion: Int, val executeSingleStep: Int = -1) {

  val versionMap = Map("600-609" -> classOf[UpgradeTo0610], "610-699" -> classOf[UpgradeTo0700])

  val stepMap = Map(3 -> "600-609", 17 -> "610-699")


  def getInstancesForFullUpgrade(upgradeChain: mutable.MutableList[UpgradeHandler]) = {
    versionMap.foreach(
      {
        pair =>
          val versionPair = pair._1.split("-").map(_.toInt)
          if (is_overlapping(versionPair(0), versionPair(1), fromVersion, toVersion)) {
            upgradeChain += pair._2.getDeclaredConstructor(classOf[Workspace]).newInstance(Helpers.getWorkspaceByDatabase(etldbname))
          }
      }
    )

  }

  def getInstanceForSingleStepUpgrade(upgradeChain: mutable.MutableList[UpgradeHandler]) = {
    stepMap.foreach({
      step =>
        if (executeSingleStep <= step._1) {
          upgradeChain += versionMap.get(step._2).get.getDeclaredConstructor(classOf[Workspace], classOf[Int], classOf[Int]).newInstance(Helpers.getWorkspaceByDatabase(etldbname), executeSingleStep.asInstanceOf[AnyRef], executeSingleStep.asInstanceOf[AnyRef])
        }
    })

  }

  def getUpgradeChain(upgradeChain: mutable.MutableList[UpgradeHandler]): Unit = {

    executeSingleStep match {
      case -1 => getInstancesForFullUpgrade(upgradeChain)
      case _ => getInstanceForSingleStepUpgrade(upgradeChain)

    }
  }

  def execute(): mutable.MutableList[DataFrame] = {
    val upgradeChain = mutable.MutableList[UpgradeHandler]()
    val upgradeResults = mutable.MutableList[DataFrame]()
    getUpgradeChain(upgradeChain)

    val upgradePath: Array[String] = Array()
    upgradeChain.foreach(f => upgradePath + f.getClass.getCanonicalName)
    println(s"Upgrade Path ${upgradePath.mkString("->")}")
    /*upgradeChain.foreach({
      f =>
        upgradeResults += f.upgrade()
    })
*/


    upgradeResults
  }
   def is_overlapping(x1:Int,x2:Int,y1:Int,y2:Int):Boolean= {
    Math.max(x1, y1) <= Math.min(x2, y2)
  }
}
object Test {
  def main(args: Array[String]): Unit = {

  }
}
