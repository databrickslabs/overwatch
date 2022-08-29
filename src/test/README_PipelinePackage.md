## How to test InitializeTest.Scala in Overwatch environment?
By default, below test cases inside InitializeTest.Scala is ignored because those need PAT which is highly confidential to execute and putting the
actual PAT inside the code is not recommended.

1. "Tests for initialize database"
2. "Tests for validateAuditLogConfigs configs"
3. "Tests for validateAndRegisterArgs function"
4. "validateAndRegisterArgs function should fail when a parameter is missing"

## How to Enable Test inside InitializeTest.scala
Follow the below steps
1. In Build.sbt for the below mentioned snippet provide the below details:

   ```
    fork in Test := true'''
    envVars in Test := Map("OVERWATCH_ENV" -> " ","OVERWATCH_TOKEN" -> " ","OVERWATCH" -> "")
   ```

    ###### For OVERWATCH_ENV provide the overwatch environment like 'demo'
    ###### For OVERWATCH_TOKEN provide the PAT token 
    ###### For OVERWATCH provide the the test enviroment like 'LOCAL'

3. For testing clusters please make sure the provided cluster is up and running.

