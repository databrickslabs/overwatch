package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.utils.{BadConfigException, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}

/**
 * Main class for running multiclass deployment.
 */
object MultiWorkspaceRunner  extends SparkSessionWrapper{

  private val logger: Logger = Logger.getLogger(this.getClass)

  private def validateInputZone(zones: String): Unit = {
    val zoneArray = zones.split(",").distinct
    zoneArray.foreach(zone => {
      val layer = zone.toLowerCase()
      if (layer == "bronze" || layer == "silver" || layer == "gold") {
          //validated
      }else{
        val errMsg = s"Unknown Zone found ${zones}, Zone should be either Bronze,Silver or Gold"
        throw new BadConfigException(errMsg)
      }
    })
  }

  /**
   * args(0) = Config file path (Mandatory)
   * args(1) =  parallelism (Optional)(Default value 4)
   * args(2) = Deployment zone(Optional)(Default Bronze,Silver,Gold)
   * @param args
   */
  def main(args: Array[String]): Unit = {
    envInit()
    if (args.length == 1) { //Deploy Bronze,Silver and Gold with default parallelism.
      logger.log(Level.INFO, "Deploying Bronze,Silver and Gold")
      MultiWorkspaceDeployment(args(0)).deploy(4,"Bronze,Silver,Gold")

    } else if (args.length == 2) {//Deploy Bronze,Silver and Gold with provided parallelism.
      val parallelism = args(1).toInt
      logger.log(Level.INFO, s"Deploying Bronze,Silver and Gold with parallelism: ${parallelism}")
      MultiWorkspaceDeployment(args(0)).deploy(parallelism,"Bronze,Silver,Gold")
    } else if(args.length == 3) {
      val parallelism = args(1).toInt
      validateInputZone(args(2))
      logger.log(Level.INFO, s"Deploying ${args(2)} with parallelism: ${parallelism}")
      MultiWorkspaceDeployment(args(0)).deploy(parallelism,args(2).toLowerCase)

    }else{
      val errMsg = s"Main class requires 1/2/3 argument, received ${args.length} arguments. Please review the " +
        s"docs to compose the input arguments appropriately."
      throw new BadConfigException(errMsg)
    }
  }

}
