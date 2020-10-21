## Build instructions

This project uses Databricks-specific jars that contain more functionality compared to open source Spark. Because we're limited to some jars compiled with Scala 2.11, we need to use Databricks Runtime 6.x.  To be able to compile this project you need to do following:

* Install Databricks Connect & Databricks CLI (better into a separate virtual Python/Conda environment - in this example, with name `overwatch`.  For current version we must to use Python 3.7!):

```sh
conda create --name overwatch python=3.7
conda activate overwatch
pip install -U databricks-connect==6.6 databricks-cli
export DBCONNECT_JARS=$(databricks-connect get-jar-dir)
```

* (optional) Configure Databricks Connect as described in [documentation](https://docs.databricks.com/dev-tools/databricks-connect.html) - it
  could be useful if you want to run the artifact from your machine
* (optional) Configure Databricks CLI as described in [documentation](https://docs.databricks.com/dev-tools/cli/index.html)
* (optional) Setting the location of the DB jars.  It could be configured by multiple ways, in order of execution:
  1. SBT will try to use the path from the environment variable `DBCONNECT_JARS` set above. 
  1. try to get path from the `DbConnectJars` system property.  It could be set in the `.sbtopts` file, for example, as `-DDbConnectJars=....`
  1. try to execute `databricks-connect get-jar-dir` if the `databricks-connect` is in the `PATH`
  1. take path from the `jarsPathManual` that is defined in the `build.sbt` - open it in the editor, and set this variable to the path to jars obtained via `databricks-connect get-jar-dir`
* Execute `sbt clean package` to build the project

