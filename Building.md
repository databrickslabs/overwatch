## Environment Setup

### All the dependencies should be in sync with  Databricks runtime 10.4 LTS.

- Java : Zulu 8.56.0.21
- Scala : 2.12.14

## SBT Installation instructions

- SBT : 1.2.8

#### Note: There can be 2 ways of installing sbt, first is to install sbt on the system and then use it through terminal. Second method is that we install the sbt in the IDE like intellij and use a sbt plugin. In either case please make sure that the sbt installation is pointing to the right java installation.

sbt installation instructions for the mac can be found at [here](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Mac.html)

sbt installation instructions for the windows can be found at [here](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Windows.html)

A detailed sbt plugin guide for IntelliJ with screenshots can be found [here](https://www.jetbrains.com/help/idea/sbt-support.html)

If you are struggling to use right java version on mac a helpful post can be found [here](https://stackoverflow.com/questions/71890689/sbt-wont-install-correctly-homebrew-changing-java-versions)

## Build instructions

sbt clean package <br>
sbt clean assembly