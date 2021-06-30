---
title: FAQ
date: 2021-01-12T05:49:17-05:00
weight: 5
---

### Here you will find some common questions and their answers with respect to the implementation of Overwatch
#### Note: All answers assume a workspace is built and running.

**Question 1: I'm attempting to rebuild an environment from scratch, I have removed the tables from the database and I'm still encountering an error**

Answer: The tables may have been dropped, but since they are external tables, the data in the corresponding paths is still present and needs to be recursively removed
Use the below command to remove the path:
```
dbutils.fs.rm("<table_name>", true) // replace table_name with the table you would like to delete
```

**Question 2: 
I have deployed Overwatch before but I would like to re-build it. I have cleaned up the databases but I'm encountering
the error "Azure Event Hub Paths are not empty on first run"**

Answer: While initating the first run, the below command should be empty: 
```
display(dbutils.fs.ls(s"${storagePrefix}/${workspaceID}").toDF)
```
If it is not empty then there is data present that was not deleted upon dropping databases.

Note : Azure Databricks uses the checkpoint directory to ensure correct and consistent progress information and it
exists in the path 'StoragePrefix/WorkspaceID/ehState/rawEventsCheckpoint'.


**Question 3: I have entered the incorrect cost/I would like to customise the costs, how do I go about that?**

Answer: Please refer to the [Configuring Custom Costs]({{%relref "GettingStarted"%}}#configuring-custom-costs).
It is important to note that if you would you'd like to customize compute costs based on your region/contract price.
You would have to initialize the pipeline which creates the instanceDetails table, which you can alter according to your needs. 
Use the command below - initialize the pipeline, change the costs, and then build it by adding '.run()' to the same command.
```
Bronze(workspace)
```