## Build instructions

sbt clean package <br>
sbt clean assembly

### Debugging Build issues:
##### Error: /packages cannot be represented as URI
    error while loading String, class file '/modules/java.base/java/lang/String.class' is broken
**Fix**: This is a known issue as SBT supports limited number of JDK Versions. Check which version SBT is [recommending](https://www.scala-sbt.org/1.x/docs/Setup.html). Uninstall, your current JDK version and install the recommended JDK version. You can download the recommended version from [here](https://adoptopenjdk.net/) .

 
