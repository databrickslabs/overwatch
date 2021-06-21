---
title: FAQ
date: 2021-01-12T05:49:17-05:00
weight: 4
---

## Here you will find some common questions and their answers with respect to the implementation of Overwatch

### Q: I have dropped the tables and I'm attempting to rebuild them, but the data does not change upon rebuilding. 
#### Answer: The tables must be dropped, but the **data is not deleted upon dropping the table**. The data must be removed recursively so the fast drop is followed by the dbutils.fs.rm command to recursively drop the folder. 

### Q: While installing overwatch, the error "Azure Event Hub Paths are not empty on first run"
#### Answer: While initating the first run, the below command should be empty display(dbutils.fs.ls(s"${storagePrefix}/${workspaceID}").toDF)
#### If it is not empty then there is data present that was not deleted upon dropping databases.
#### Here is an [example notebook](https://databrickslabs.github.io/overwatch/assets/FAQ/FAQ1&2.dbc). The example I've used is **customising costs**
