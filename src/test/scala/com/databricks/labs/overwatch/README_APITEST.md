## How to test API in Overwatch environment?
ApiCallV2Test.scala can be used to test different APIs.
By default, all the test inside ApiCallV2Test.scala is ignored because it needs PAT which is highly confidential to execute and putting the
actual PAT inside the code is not recommended.

## How to Enable Test inside ApiCallV2Test.scala 
Follow the below steps

1. Create ApiEnv object by giving appropriate workspace URL and PAT.
`EX : apiEnv = ApiEnv(false, <workspace URL>, <PAT>, pkgVr)`
2. For testing sql/history/queries please make sure the workspace should have sql endpoint enabled.
3. For testing clusters/resize please make sure the provided cluster is up and running.


