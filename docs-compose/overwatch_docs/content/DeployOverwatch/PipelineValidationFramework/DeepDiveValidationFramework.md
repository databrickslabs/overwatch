---
title: "Deep Dive Validation Framework"
date: 2024-03-16T18:06:39+05:30
---

This pipeline validation module will run after overwatch deployment is done.For now,this module will be used by the support team for checking the data quality of recent Deployments.

{{% notice note %}}
Upon execution, the Validation Framework is designed to process only those records pending validation, thereby 
ensuring that previously validated records are not subjected to redundant validation checks. For instance, 
following a fresh deployment, executing the Validation Framework will initiate validation exclusively for 
records associated with the recent deployment. Subsequently, if the framework is executed after the second and third 
deployments, it will selectively validate records related to these deployments that have not yet been validated, 
thus maintaining the efficiency and relevance of the validation process.
{{% /notice %}}

### High Level Design for Validation Framework
Here is the high level design of the framework
 ![PipelineValidationFramework](/images/DeployOverwatch/Pipeline_Validation.png)

1. Overwatch Deployment is finished as per the usual process.
2. Trigger the Validation Framework and aS per the rules defined in Validation Framework the data in tables would be validated.
3. This decision point checks the results of the pipeline validation
4. After validation framework is executed, a health check report for the pipeline is generated. This report likely
   contains details on the validation checks and indicates that the pipeline is healthy and functioning as expected. 
5. If any validation check fails, the snapshot of rows that got failed are moved to a quarantine zone. This would be used for 
   debugging purpose.

