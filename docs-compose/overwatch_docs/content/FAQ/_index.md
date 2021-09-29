---
title: FAQ
date: 2021-01-12T05:49:17-05:00
weight: 5
---

## Using the FAQs
The answers and examples to the questions below assume that a workspace is instantiated and ready to use.
There are many ways to instantiate a workspace, below is a simple way but it doesn't matter how you create the
Workspace, just so long as the state of the workspace and/or pipeline can be referenced and utilized.
A simple example is below. There are much more verbose details in the
[Advanced Topics Section]({{%relref "GettingStarted/AdvancedTopics.md"%}}/#interacting-with-overwatch-and-its-state)
if you'd like a deeper explanation.
```scala
import com.databricks.labs.overwatch.utils.Upgrade
import com.databricks.labs.overwatch.pipeline.Initializer
val params = OverwatchParams(...)
val prodArgs = JsonUtils.objToJson(params).compactString

// A more verbose example is available in the example notebooks referenced above
val prodWorkspace = Initializer(prodArgs, debugFlag = true)
val upgradeReport = Upgrade.upgradeTo042(prodWorkspace)
display(upgradeReport)
```

## Q 1: I just deployed Overwatch and made a mistake and want to clean up everything and re-deploy. I can't seem to get a clean state from which to begin though.
Overwatch tables are intentionally created as external tables. When the
[etlDataPathPrefix]({{%relref "GettingStarted/Configuration.md"%}}/#datatarget) is configured (as recommended)
the target data does not live underneath the database directory and as such is not deleted when the database is dropped.
This is considered an "external table" and this is done by design to protect the org from someone accidentally dropping
all data globally. For a FULL cleanup you may use the functions below but these are destructive, be sure that
you review it and fully understand it before you run it.

{{% notice warning %}}
**CRITICAL**: **CHOOSE THE CORRECT FUNCTION FOR YOUR ENVIRONMENT**
If Overwatch is configured on multiple workspaces, use the multi-workspace script otherwise, use the
single workspace functions. Please do not remove the safeguards as they are there to protect you and your company from
accidents.
{{% /notice %}}

**SINGLE WORKSPACE DEPLOYMENT DESTROY AND REBUILD**

[single_workspace_cleanup.dbc](/assets/FAQ/single_workspace_cleanup.dbc) |
[single_workspace_cleanup.html](/assets/FAQ/single_workspace_cleanup.html)

**MULTI WORKSPACE DEPLOYMENT DESTROY AND REBUILD**

First off, don't practice deploy a workspace to the global Overwatch data target as it's challenging to clean-up;
nonetheless, the script below will remove the data from this workspace from the global dataset and reset this
workspace for a clean run.

[multi_workspace_cleanup.dbc](/assets/FAQ/multi_workspace_cleanup.dbc) |
[multi_workspace_cleanup.html](/assets/FAQ/multi_workspace_cleanup.html)

## Q 2: The incorrect costs were entered for a specific time range, I'd like to correct them, how should I do that?
Please refer to the [Configuring Custom Costs]({{%relref "GettingStarted"%}}#configuring-custom-costs)
to baseline your understanding. Also, closely review the details of the
[instanceDetails]({{%relref "DataEngineer/Definitions.md"%}}/#instancedetails)
table definition as that's where costs are stored and this is what will need to be accurate. Lastly,
please refer to the [ETL_Process]({{%relref "DataEngineer/ETL_Process.md"%}}) section of the docs as
you'll likely need to understand how to move the pipeline through time.

Now that you have a good handle on the costs and the process for restoring / reloading data, note that there are
ultimately two options after you reconfigure the instancedetails table to be accurate. There are two tables that
are going to have inaccurate data in them:
* jobruncostpotentialfact_gold
* clusterstatefact

**Options**
1. Completely drop tables, delete their underlying data and rebuild them from the primordial date
2. Delete all records where Pipeline_SnapTS >= the time where the data became inaccurate.

Both options will require a corresponding appropriate update to the pipeline_report table for the two modules
that write to those tables (3005, 3015). [Here are several examples]({{%relref "DataEngineer/ETL_Process.md"%}}/#reloading-data-example)
on ways to update the pipeline_report to change update the state of a module.

## Q 3: What is the current guidance / state of customers who want to capture EC2 spot pricing?

The easiest way is to identify an "average" SPOT price and apply it to all compute cost calcuations. Going a bit further, calculate average spot price by region, average spot price by region by node type and lastly down to a time resolution like hour of day. Use the AWS APIs to get the historical spot price at by these dims.This can be automated via a little script they build but it require proper access to the AWS account.

```scala
display(
  spark.table("overwatch.clusterstatefact")
    .withColumn("compute_cost", applySpotDiscount('awsRegion, 'nodeTypeID)
)
```
where **applySpotDiscount** is the lookup function that either hits a static table of estimates or a live API endpoint from AWS.

It's a very simple API call,just need the token to do it.It's extremely powerful for shaping/optimizing job run timing, node types, AZs, etc

Sample Request:
```https://ec2.amazonaws.com/?Action=DescribeSpotPriceHistory
&StartTime=2020-11-01T00:00:00.000Z
&EndTime=2020-11-01T23:59:59.000Z
&AvailabilityZone=us-west-2a
&AUTHPARAMS
```
Sample Response:
```<DescribeSpotPriceHistoryResponse xmlns="http://ec2.amazonaws.com/doc/2020-11-15/">
  <requestId>59dbff89-35bd-4eac-99ed-be587EXAMPLE</requestId>
  <spotPriceHistorySet>
    <item>
      <instanceType>m3.medium</instanceType>
      <productDescription>Linux/UNIX</productDescription>
      <spotPrice>0.287</spotPrice>
      <timestamp>2020-11-01T20:56:05.000Z</timestamp>
      <availabilityZone>us-west-2a</availabilityZone>
    </item>
    <item>
      <instanceType>m3.medium</instanceType>
      <productDescription>Windows</productDescription>
      <spotPrice>0.033</spotPrice>
      <timestamp>2020-11-01T22:33:47.000Z</timestamp>
      <availabilityZone>us-west-2a</availabilityZone>
    </item>
  </spotPriceHistorySet>
  <nextToken/>
</DescribeSpotPriceHistoryResponse>
```
More Details: [https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeSpotPriceHistory.html](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeSpotPriceHistory.html)



## Q 4: How to config the cluster logs path?
The cluster logging paths are automatically acquired within Overwatch. There's no need to tell Overwatch where to load those from.
[Click here]({{%relref "EnvironmentSetup/azure.md"%}}/#configuring-overwatch-on-azure-databricks) for more details.
![FAQ4](/images/_index/FAQ/faq4.png)

## Q 5: Why i couldn't get data in the Bronze_ClusterEventLogs and getting Empty/Failed ?
![FAQ5](/images/FAQ/clustereventlogs.png)

Consider this example, the primordinal date was 06-22-2021 and deployment date was 09-21-2021.
If you look at the **Bronze_ClusterEventLogs** (1005), it was EMPTY because it will never load more than 30 days data from the API. Always check the **untilTSt** column, Whenever we run overwatch, **untilTSt** column should be the present day. In this case, it stops at 08-22-2021. Gold_ClusterStateFact will also be empty, because it depends on Bronze_ClusterEventLogs.  

**To Do:**  

It can be done in 2 ways,
1. Try changing the Primordial Date widget in the runner notebook like, today minus primordinal date(i.e, day difference between Sept-21 and June-22 is 93 days)
2. Otherwise, change the Max Days widget to 30 and run the **Bronze(workspace).run()** multiple times, to get bronze data till current date

If using notebook, clone the runner notebook and delete everything after Bronze(workspace).run(), except the display(pipReport) and check the untilTSt column, whether you are getting data till current day.

If using Main class, add an argument to the json array string [“bronze”, “...<escaped_string>…”] and check the output.
![main_class](/images/FAQ/main_class.png)
