#API
Overwatch gets the information by consuming data from different APIs provided by Databricks.

#Testing API
ApiCallV2Test.scala can be used to test different APIs.
By default, test inside ApiCallV2Test.scala is ignored because it needs PAT which is highly confidential to call different APIs.

#Enabling Test inside ApiCallV2Test.scala 
create ApiEnv object by giving appropriate workspace URL and PAT.
EX : apiEnv = ApiEnv(false, <workspace URL>, <PAT>, pkgVr)

For testing sql/history/queries please make sure the workspace should have sql endpoint enabled.
For testing clusters/resize please make sure the provided cluster is up and running.


