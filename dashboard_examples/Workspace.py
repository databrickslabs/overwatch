# Databricks notebook source
# MAGIC %md
# MAGIC # Overwatch Queries - Workspace Analysis
# MAGIC * Overwatch Version 0.6.1.0
# MAGIC 
# MAGIC ### Context
# MAGIC Below queries are developed using Pyspark and visualised using plotly. This notebook is primarily focused on Workspace analysis. Initially walk through over user info and later defining the cluster costs based on cluster categories.
# MAGIC 
# MAGIC ### Table of Contents
# MAGIC #### Workspace Overview
# MAGIC  
# MAGIC * User count having admin access
# MAGIC * Access details that a user has
# MAGIC * DBPAT details and its purpose
# MAGIC * User recorded activity
# MAGIC 
# MAGIC #### Workspace level cluster types
# MAGIC 
# MAGIC * Interactive cluster
# MAGIC * Automated Cluster
# MAGIC * SQL Cluster
# MAGIC 
# MAGIC ####  Cost variation over workspace level
# MAGIC 
# MAGIC * Weekly change over Interactive, Automated and SQL clusters
# MAGIC * Monthly change over Interactive, Automated and SQL clusters

# COMMAND ----------

# MAGIC %md
# MAGIC #### Comments
# MAGIC 
# MAGIC ######Pending Tasks
# MAGIC * Calculating weekly cost for interactive, automated and SQL clusters
# MAGIC * Gathering the Admin access and user access list in all workspaces
# MAGIC * DBPAT details

# COMMAND ----------

# MAGIC %run "/Dashboards_Dev/In Progress/Helpers"

# COMMAND ----------

from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ###User Access over Workspace
# MAGIC 1. Admin users - List of user who have the admin access ( access to the workspace)
# MAGIC 2. User's access details - List of access that a individual has in the workspace
# MAGIC 3. DBPAT details and usage - DBPAT created details and its purpose of usage

# COMMAND ----------

# MAGIC %md
# MAGIC ###User Login activity on each workspace
# MAGIC 1. 30 days
# MAGIC 2. 60 days
# MAGIC 3. 90 days

# COMMAND ----------

# MAGIC %sql
# MAGIC -- sparkJob, if there are any records for the userEmail, then yes, that user has used it 
# MAGIC 
# MAGIC select *, 
# MAGIC (case when last_login_date > current_date() - 30 then True else False end) as login_last_30_days,
# MAGIC (case when last_login_date > current_date() - 60 then True else False end) as login_last_60_days,
# MAGIC (case when last_login_date > current_date() - 90 then True else False end) as login_last_90_days
# MAGIC from
# MAGIC (select login_user,
# MAGIC last(login_date) as last_login_date
# MAGIC from overwatch_global_etl.accountlogin
# MAGIC group by 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Interactive Cluster Cost/workspace level
# MAGIC 1. Daily Cost - Daily Interactive cluster cost on each workspace for the past 30 days
# MAGIC 2. Weekly Cost - Weekly Interactive cluster cost on each workspace for the past few weeks ( 1 year )
# MAGIC 3. Monthly Cost - Montly Interactive cluster cost on each workspace for the past 1 year

# COMMAND ----------

# Top 25 cluster spend broken down by workspace - Last 30 days

# Dataframe which calculate the overall cost for each workspace.
totalCost = clusterstatefact\
.withColumn("date", clusterstatefact["timestamp_state_start"].cast("date"))\
.withColumn("organization_id", col("organization_id"))\
.where(col('cluster_name') != 'SQLAnalytics')

# Dataframe with previous 30 days, calculate the overall cost for each workspace.
prev30days = totalCost\
.join(clusterstatefact_max, totalCost.date > clusterstatefact_max.max_date - 31, "inner")\
.groupBy(["date", "organization_id",
          "workspace_name",
          "isAutomated"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))

a = prev30days\
.withColumn('cluster_type', expr("case when isAutomated = 'false' then 'interactive'" + "when isAutomated = 'true' then 'automated' else 'cluster_name' end"))\
.where(col('isAutomated') == 'false')\
.select('organization_id', 'workspace_name', 'Total_Cost (USD)', 'isAutomated', 'cluster_type', "date")

# display(a)

# Converting pyspark to pandas for visualization
prev30Days_pandas = a.toPandas()

# # Plotting dataframe view using plotly library
fig = px.bar(prev30Days_pandas,
             x="date",
             y = "Total_Cost (USD)", 
             color = "workspace_name", 
             hover_data = ["organization_id",
                           "workspace_name"
                          ],
             title = "Interactive Cluster spend broken down by workspace - Last 30 days")

# # Visualizing the graph
fig.show()

# COMMAND ----------

# Top 25 cluster spend broken down by workspace - Last 30 days

# Dataframe which calculate the overall cost for each workspace.

totalCost = clusterstatefact\
.withColumn("quarter", date_format(col('timestamp_state_start'), 'Q'))\
.withColumn('year', year(clusterstatefact.timestamp_state_start))\
.withColumn("organization_id", col("organization_id"))\
.where(col('cluster_name') != 'SQLAnalytics')

# Dataframe with previous 30 days, calculate the overall cost for each workspace.
prev30days = totalCost\
.groupBy(["organization_id",
          "workspace_name",
          "isAutomated", "year", "quarter"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.where((col('isAutomated') == False) & (col('year') == 2022))\
.orderBy(col('year').desc(), col('quarter').desc())

# # Converting pyspark to pandas for visualization
prev30Days_pandas = prev30days.toPandas()

# # Plotting dataframe view using plotly library
fig = px.bar(prev30Days_pandas,
             x="quarter",
             y = "Total_Cost (USD)", 
             color = "workspace_name", 
             hover_data = ["organization_id",
                           "workspace_name"
                          ],
             title = "Interactive Cluster spend broken down by workspace - Quarter wise")

# # Visualizing the graph
fig.show()
# display(prev30days)

# COMMAND ----------

# Top 25 cluster spend broken down by workspace - Last 30 days

# Dataframe which calculate the overall cost for each workspace.

totalCost = clusterstatefact\
.withColumn('month', month(clusterstatefact.timestamp_state_start))\
.withColumn('year', year(clusterstatefact.timestamp_state_start))\
.withColumn("organization_id", col("organization_id"))\
.where(col('cluster_name') != 'SQLAnalytics')

# Dataframe with previous 30 days, calculate the overall cost for each workspace.
prev30days = totalCost\
.groupBy(["year", "month", "organization_id",
          "workspace_name",
          "isAutomated"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.where((col('isAutomated') == False) & (col('year') == 2022))

month_lst = ['January', 'Feburary', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

name = 'month'

udf = UserDefinedFunction(lambda x: month_lst[int(x%12) - 1], StringType())
new_df = prev30days.select(*[udf(column).alias(name) if column == name else column for column in prev30days.columns])\
.orderBy(col('year').desc(), col('month').desc())

# Converting pyspark to pandas for visualization
prev30Days_pandas = new_df.toPandas()

# Plotting dataframe view using plotly library
fig = px.bar(prev30Days_pandas,
             x="month",
             y = "Total_Cost (USD)", 
             color = "workspace_name", 
             hover_data = ["organization_id",
                           "workspace_name"
                          ],
             title = "Interactive Cluster spend broken down by workspace - Month wise")

# # Visualizing the graph
fig.show()
# display(new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Automated Cluster Cost/workspace level
# MAGIC 1. Daily Cost - Daily Automated cluster cost on each workspace for the past 30 days
# MAGIC 2. Weekly Cost - Weekly Automated cluster cost on each workspace for the past few weeks ( 1 year )
# MAGIC 3. Monthly Cost - Montly Automated cluster cost on each workspace for the past 1 year

# COMMAND ----------

# Top 25 cluster spend broken down by workspace - Last 30 days

# Dataframe which calculate the overall cost for each workspace.
totalCost = clusterstatefact\
.withColumn("date", clusterstatefact["timestamp_state_start"].cast("date"))\
.withColumn("organization_id", col("organization_id"))\
.where(col('cluster_name') != 'SQLAnalytics')

# Dataframe with previous 30 days, calculate the overall cost for each workspace.
prev30days = totalCost\
.join(clusterstatefact_max, totalCost.date > clusterstatefact_max.max_date - 31, "inner")\
.groupBy(["date", "organization_id",
          "workspace_name",
          "isAutomated"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.where((col('isAutomated') == True))\
.orderBy(col('date').desc())


# display(a)

# Converting pyspark to pandas for visualization
prev30Days_pandas = prev30days.toPandas()

# # Plotting dataframe view using plotly library
fig = px.bar(prev30Days_pandas,
             x="date",
             y = "Total_Cost (USD)", 
             color = "workspace_name", 
             hover_data = ["organization_id",
                           "workspace_name"
                          ],
             title = "Automated Cluster spend broken down by workspace - Last 30 days")

# # Visualizing the graph
fig.show()

# COMMAND ----------

# Top 25 cluster spend broken down by workspace - Last 30 days

# Dataframe which calculate the overall cost for each workspace.
totalCost = clusterstatefact\
.withColumn("quarter", date_format(col('timestamp_state_start'), 'Q'))\
.withColumn('year', year(clusterstatefact.timestamp_state_start))\
.withColumn("organization_id", col("organization_id"))\
.where(col('cluster_name') != 'SQLAnalytics')

# Dataframe with previous 30 days, calculate the overall cost for each workspace.
prev30days = totalCost\
.groupBy(["year", "quarter", "organization_id",
          "workspace_name",
          "isAutomated"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.where((col('isAutomated') == True) & (col('year') == 2022))\
.orderBy(col('year').desc(), col('quarter').desc())


# display(a)

# Converting pyspark to pandas for visualization
prev30Days_pandas = prev30days.toPandas()

# # Plotting dataframe view using plotly library
fig = px.bar(prev30Days_pandas,
             x="quarter",
             y = "Total_Cost (USD)", 
             color = "workspace_name", 
             hover_data = ["organization_id",
                           "workspace_name"
                          ],
             title = "Automated Cluster spend broken down by workspace - Quarter wise")

# # Visualizing the graph
fig.show()

# COMMAND ----------

# Top 25 cluster spend broken down by workspace - Last 30 days

# Dataframe which calculate the overall cost for each workspace.
totalCost = clusterstatefact\
.withColumn('month', month(clusterstatefact.timestamp_state_start))\
.withColumn('year', year(clusterstatefact.timestamp_state_start))\
.withColumn("organization_id", col("organization_id"))\
.where(col('cluster_name') != 'SQLAnalytics')

# Dataframe with previous 30 days, calculate the overall cost for each workspace.
prev30days = totalCost\
.groupBy(["year", "month", "organization_id",
          "workspace_name",
          "isAutomated"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.where((col('isAutomated') == True) & (col('year') == 2022))

month_lst = ['January', 'Feburary', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

name = 'month'

udf = UserDefinedFunction(lambda x: month_lst[int(x%12) - 1], StringType())
new_df = prev30days.select(*[udf(column).alias(name) if column == name else column for column in prev30days.columns])\
.orderBy(col('year').desc(), col('month').desc())

# Converting pyspark to pandas for visualization
prev30Days_pandas = new_df.toPandas()

# # Plotting dataframe view using plotly library
fig = px.bar(prev30Days_pandas,
             x="month",
             y = "Total_Cost (USD)", 
             color = "workspace_name", 
             hover_data = ["organization_id",
                           "workspace_name"
                          ],
             title = "Automated Cluster spend broken down by workspace - Last 30 days")

# # Visualizing the graph
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SQL Cluster Cost/workspace level
# MAGIC 1. Daily Cost - Daily SQL cluster cost on each workspace for the past 30 days
# MAGIC 2. Weekly Cost - Weekly SQL cluster cost on each workspace for the past few weeks ( 3 months )
# MAGIC 3. Monthly Cost - Montly SQL cluster cost on each workspace for the past 1 year

# COMMAND ----------

# Top 25 cluster spend broken down by workspace - Last 30 days

df = clusterstatefact.withColumn("date", clusterstatefact["timestamp_state_start"].cast("date"))\
.withColumn('SqlEndpointId', json_tuple(col("custom_tags"), "SqlEndpointId"))\
.where(col('cluster_name') == 'SQLAnalytics')

dff = df.join(clusterstatefact_max, df.date > clusterstatefact_max.max_date - 31, "inner")\
.where(col('SqlEndpointId') != 'null')\
.groupBy(["date", "organization_id",
          "workspace_name"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)")).select('date', 'organization_id', 'workspace_name', 'Total_Cost (USD)')

# display(dff)

prev30Days_pandas = dff.toPandas()

# # Plotting dataframe view using plotly library
fig = px.bar(prev30Days_pandas,
             x="date",
             y = "Total_Cost (USD)", 
             color = "workspace_name", 
             hover_data = ["organization_id",
                           "workspace_name"
                          ],
             title = " SQL Cluster spend broken down by workspace - Last 30 days")

# Visualizing the graph
fig.show()

# COMMAND ----------

df = clusterstatefact.withColumn("quarter", date_format(col('timestamp_state_start'), 'Q'))\
.withColumn('year', year(clusterstatefact.timestamp_state_start))\
.withColumn('SqlEndpointId', json_tuple(col("custom_tags"), "SqlEndpointId"))\
.where(col('cluster_name') == 'SQLAnalytics')

dff = df\
.where((col('SqlEndpointId') != 'null') & (col('year') == 2022))\
.groupBy(["year", "quarter", "organization_id",
          "workspace_name"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)")).select('year', 'quarter', 'organization_id', 'workspace_name', 'Total_Cost (USD)')\
.orderBy(col('year').desc(), col('quarter').desc())

prev30Days_pandas = dff.toPandas()

# # Plotting dataframe view using plotly library
fig = px.bar(prev30Days_pandas,
             x="quarter",
             y = "Total_Cost (USD)", 
             color = "workspace_name", 
             hover_data = ["organization_id",
                           "workspace_name"
                          ],
             title = " SQL Cluster spend broken down by workspace - Quarter Wise")

# Visualizing the graph
fig.show()

# COMMAND ----------

df = clusterstatefact.withColumn('month', month(clusterstatefact.timestamp_state_start))\
.withColumn('year', year(clusterstatefact.timestamp_state_start))\
.withColumn('SqlEndpointId', json_tuple(col("custom_tags"), "SqlEndpointId"))\
.where(col('cluster_name') == 'SQLAnalytics')

dff = df\
.where((col('SqlEndpointId') != 'null') & (col('year') == 2022))\
.groupBy(["year", "month", "organization_id",
          "workspace_name"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)")).select('year', 'month', 'organization_id', 'workspace_name', 'Total_Cost (USD)')

month_lst = ['January', 'Feburary', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

name = 'month'

udf = UserDefinedFunction(lambda x: month_lst[int(x%12) - 1], StringType())
new_df = dff.select(*[udf(column).alias(name) if column == name else column for column in dff.columns])\
.orderBy(col('year').desc(), col('month').desc())

prev30Days_pandas = new_df.toPandas()

# # Plotting dataframe view using plotly library
fig = px.bar(prev30Days_pandas,
             x="month",
             y = "Total_Cost (USD)", 
             color = "workspace_name", 
             hover_data = ["organization_id",
                           "workspace_name"
                          ],
             title = " SQL Cluster spend broken down by workspace - Month wise")

# Visualizing the graph
fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Interactive Cluster Cost Variation/workspace level
# MAGIC 1. MOM Change - Monthly interactive cluster spent variation on each workspace
# MAGIC 2. WOW Change - Weekly interactive cluster spent variation on each workspace

# COMMAND ----------

# Interactive Clusters

clusterNameSplit_int = clusterstatefact\
.withColumn("date", clusterstatefact["timestamp_state_start"].cast("date"))\
.withColumn("organization_id", col("organization_id"))\
.withColumn("ClusterName",concat_ws('_', col('cluster_name'), substring(col('organization_id'),-4,4)))\
.where((col('isAutomated') == False) & (col('cluster_name') != 'SQLAnalytics'))

#Last week cost gets breakdown by workspace name
firstWeekCost_int = clusterNameSplit_int\
.join(clusterstatefact_max, clusterNameSplit_int.date > clusterstatefact_max.max_date - 8, "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 0-7 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

#Weekly cost gets breakdown by workspace name, for the week preceding penultimate week
secondWeekCost_int = clusterNameSplit_int\
.join(clusterstatefact_max, ((clusterNameSplit_int.date > clusterstatefact_max.max_date - 15)
& (clusterNameSplit_int.date <= clusterstatefact_max.max_date - 8)), "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 8-14 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

# merging dataframes
unionOfCluster_int = firstWeekCost_int.unionByName(secondWeekCost_int)

# window = Window.partitionBy(unionOfCluster["organization_id"]).orderBy(unionOfCluster['Total_Cost (USD)'].desc())
# rank = union.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)

w1 = Window.orderBy("organization_id", col("time_period").desc())
lagCol_int = lag(col("Total_Cost (USD)"), 1).over(w1)

# #a dataframe that have values of last three weeks
totalClusterCost_int = unionOfCluster_int\
.withColumn("lm_total_cost", lagCol_int)\
.orderBy("time_period")\
.withColumn("Wow_change (USD)", round((col("Total_Cost (USD)") - col("lm_total_cost")), 2))\
.withColumn("WoW_change (%)", round(((col("Total_Cost (USD)") - col("lm_total_cost")) / col("lm_total_cost") * 100), 2))\
.where((col("time_period") != ("Last 8-14 days")) & (col("Total_Cost (USD)")>50))\
.orderBy(col("WoW_change (%)").desc())\
.select("time_period",
"organization_id",
"workspace_name",
"Total_Cost (USD)",
"lm_total_cost",
"WoW_change (%)",
"Wow_change (USD)")

window = Window.partitionBy(totalClusterCost_int["organization_id"]).orderBy(totalClusterCost_int['Total_Cost (USD)'].desc())
r1 = totalClusterCost_int.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)

# Last three weeks data converted to pandas for visualization
totalCost_int = r1.toPandas()



# Plotting dataframe view using plotly library
fig = px.bar(totalCost_int,
x = "workspace_name",
y = "WoW_change (%)",
hover_data = ["organization_id",
"workspace_name", "Wow_change (USD)", "Total_Cost (USD)"],
color = "workspace_name",
title = "Weekly Interactive cluster spend variation across all workspaces (top 5)")
fig.show()

# COMMAND ----------

# Interactive Clusters

clusterNameSplit_int = clusterstatefact\
.withColumn("date", clusterstatefact["timestamp_state_start"].cast("date"))\
.withColumn("organization_id", col("organization_id"))\
.withColumn("ClusterName",concat_ws('_', col('cluster_name'), substring(col('organization_id'),-4,4)))\
.where((col('isAutomated') == False) & (col('cluster_name') != 'SQLAnalytics'))

#Last week cost gets breakdown by workspace name
firstWeekCost_int = clusterNameSplit_int\
.join(clusterstatefact_max, clusterNameSplit_int.date > clusterstatefact_max.max_date - 31, "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 1-30 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

#Weekly cost gets breakdown by workspace name, for the week preceding penultimate week
secondWeekCost_int = clusterNameSplit_int\
.join(clusterstatefact_max, ((clusterNameSplit_int.date > clusterstatefact_max.max_date - 61)
& (clusterNameSplit_int.date <= clusterstatefact_max.max_date - 31)), "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 31-60 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

# merging dataframes
unionOfCluster_int = firstWeekCost_int.unionByName(secondWeekCost_int)

# window = Window.partitionBy(unionOfCluster["organization_id"]).orderBy(unionOfCluster['Total_Cost (USD)'].desc())
# rank = union.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)

w1 = Window.orderBy("organization_id", col("time_period").desc())
lagCol_int = lag(col("Total_Cost (USD)"), 1).over(w1)

# #a dataframe that have values of last three weeks
totalClusterCost_int = unionOfCluster_int\
.withColumn("lm_total_cost", lagCol_int)\
.orderBy("time_period")\
.withColumn("Wow_change (USD)", round((col("Total_Cost (USD)") - col("lm_total_cost")), 2))\
.withColumn("WoW_change (%)", round(((col("Total_Cost (USD)") - col("lm_total_cost")) / col("lm_total_cost") * 100), 2))\
.where((col("time_period") != ("Last 31-60 days")) & (col("Total_Cost (USD)")>50))\
.orderBy(col("WoW_change (%)").desc())\
.select("time_period",
"organization_id",
"workspace_name",
"Total_Cost (USD)",
"lm_total_cost",
"WoW_change (%)",
"Wow_change (USD)")

window = Window.partitionBy(totalClusterCost_int["organization_id"]).orderBy(totalClusterCost_int['Total_Cost (USD)'].desc())
r1 = totalClusterCost_int.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)

# Last three weeks data converted to pandas for visualization
totalCost_int = r1.toPandas()



# Plotting dataframe view using plotly library
fig = px.bar(totalCost_int,
x = "workspace_name",
y = "WoW_change (%)",
hover_data = ["organization_id",
"workspace_name", "Wow_change (USD)", "Total_Cost (USD)"],
color = "workspace_name",
title = "Monthly Interactive cluster spend variation across all workspaces")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Automated Cluster Cost Variation/workspace level
# MAGIC   1. MOM Change - Monthly Automated cluster spent variation on each workspace
# MAGIC 2. WOW Change - Weekly Automated cluster spent variation on each workspace.

# COMMAND ----------

# Automated Clusers

clusterNameSplit_aut = jobruncostpotentialfact\
.withColumn("date", jobruncostpotentialfact.job_runtime["startTS"].cast("date"))\
.withColumn("organization_id", col("organization_id"))

#Last week cost gets breakdown by workspace name
firstWeekCost = clusterNameSplit_aut\
.join(jobruncostpotentialfact_max, clusterNameSplit_aut.date > jobruncostpotentialfact_max.start_date - 8, "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(jobruncostpotentialfact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 0-7 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

#Weekly cost gets breakdown by workspace name, for the week preceding penultimate week
secondWeekCost = clusterNameSplit_aut\
.join(jobruncostpotentialfact_max, ((clusterNameSplit_aut.date > jobruncostpotentialfact_max.start_date - 15)
& (clusterNameSplit_aut.date <= jobruncostpotentialfact_max.start_date - 8)), "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(jobruncostpotentialfact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 8-14 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

# merging dataframes
unionOfCluster_aut = firstWeekCost.unionByName(secondWeekCost)

# window = Window.partitionBy(unionOfCluster_aut["organization_id"]).orderBy(unionOfCluster_aut['Total_Cost (USD)'].desc())
# rank = union.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)

w = Window.orderBy("organization_id", col("time_period").desc())
lagCol = lag(col("Total_Cost (USD)"), 1).over(w)

# #a dataframe that have values of last three weeks
totalClusterCost_aut = unionOfCluster_aut\
.withColumn("lm_total_cost", lagCol)\
.orderBy("time_period")\
.withColumn("Wow_change (USD)", round((col("Total_Cost (USD)") - col("lm_total_cost")), 2))\
.withColumn("WoW_change (%)", round(((col("Total_Cost (USD)") - col("lm_total_cost")) / col("lm_total_cost") * 100), 2))\
.where((col("time_period") != ("Last 8-14 days")))\
.orderBy(col("WoW_change (%)").desc())\
.select("time_period",
"organization_id",
"workspace_name",
"Total_Cost (USD)",
"lm_total_cost",
"WoW_change (%)",
"Wow_change (USD)")

window = Window.partitionBy(totalClusterCost_aut["organization_id"]).orderBy(totalClusterCost_aut['Total_Cost (USD)'].desc())
r2 = totalClusterCost_aut.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)

# Last three weeks data converted to pandas for visualization
totalCost_aut = r2.toPandas()



# Plotting dataframe view using plotly library
fig = px.bar(totalCost_aut,
x = "workspace_name",
y = "WoW_change (%)",
hover_data = ["organization_id",
"workspace_name", "Wow_change (USD)", "Total_Cost (USD)"],
color = "workspace_name",
title = "Weekly Automated cluster spend variation across all workspaces (top 5)")
fig.show()

# COMMAND ----------

# Automated Clusers

clusterNameSplit_aut = jobruncostpotentialfact\
.withColumn("date", jobruncostpotentialfact.job_runtime["startTS"].cast("date"))\
.withColumn("organization_id", col("organization_id"))

#Last week cost gets breakdown by workspace name
firstWeekCost = clusterNameSplit_aut\
.join(jobruncostpotentialfact_max, clusterNameSplit_aut.date > jobruncostpotentialfact_max.start_date - 31, "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(jobruncostpotentialfact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 1-30 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

#Weekly cost gets breakdown by workspace name, for the week preceding penultimate week
secondWeekCost = clusterNameSplit_aut\
.join(jobruncostpotentialfact_max, ((clusterNameSplit_aut.date > jobruncostpotentialfact_max.start_date - 61)
& (clusterNameSplit_aut.date <= jobruncostpotentialfact_max.start_date - 31)), "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(jobruncostpotentialfact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 31-60 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

# merging dataframes
unionOfCluster_aut = firstWeekCost.unionByName(secondWeekCost)

# window = Window.partitionBy(unionOfCluster_aut["organization_id"]).orderBy(unionOfCluster_aut['Total_Cost (USD)'].desc())
# rank = union.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)

w = Window.orderBy("organization_id", col("time_period").desc())
lagCol = lag(col("Total_Cost (USD)"), 1).over(w)

# #a dataframe that have values of last three weeks
totalClusterCost_aut = unionOfCluster_aut\
.withColumn("lm_total_cost", lagCol)\
.orderBy("time_period")\
.withColumn("Wow_change (USD)", round((col("Total_Cost (USD)") - col("lm_total_cost")), 2))\
.withColumn("WoW_change (%)", round(((col("Total_Cost (USD)") - col("lm_total_cost")) / col("lm_total_cost") * 100), 2))\
.where((col("time_period") != ("Last 31-60 days")))\
.orderBy(col("WoW_change (%)").desc())\
.select("time_period",
"organization_id",
"workspace_name",
"Total_Cost (USD)",
"lm_total_cost",
"WoW_change (%)",
"Wow_change (USD)")

window = Window.partitionBy(totalClusterCost_aut["organization_id"]).orderBy(totalClusterCost_aut['Total_Cost (USD)'].desc())
r2 = totalClusterCost_aut.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)

# Last three weeks data converted to pandas for visualization
totalCost_aut = r2.toPandas()



# Plotting dataframe view using plotly library
fig = px.bar(totalCost_aut,
x = "workspace_name",
y = "WoW_change (%)",
hover_data = ["organization_id",
"workspace_name", "Wow_change (USD)", "Total_Cost (USD)"],
color = "workspace_name",
title = "Monthly Automated cluster spend variation across all workspaces")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SQL Cluster Cost Variation/workspace level
# MAGIC 1. MOM Change - Monthly SQL cluster spent variation on each workspace
# MAGIC 2. WOW Change - Weekly SQL cluster spent variation on each workspace.

# COMMAND ----------

# SQL Clusters

clusterNameSplit_sql = clusterstatefact\
.withColumn("date", clusterstatefact["timestamp_state_start"].cast("date"))\
.withColumn("organization_id", col("organization_id"))\
.withColumn('SqlEndpointId', json_tuple(col("custom_tags"), "SqlEndpointId"))\
.where((col('cluster_name') == 'SQLAnalytics'))

#Last week cost gets breakdown by workspace name
firstWeekCost = clusterNameSplit_sql\
.join(clusterstatefact_max, clusterNameSplit_sql.date > clusterstatefact_max.max_date - 8, "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 0-7 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

#Weekly cost gets breakdown by workspace name, for the week preceding penultimate week
secondWeekCost = clusterNameSplit_sql\
.join(clusterstatefact_max, ((clusterNameSplit_sql.date > clusterstatefact_max.max_date - 15)
& (clusterNameSplit_sql.date <= clusterstatefact_max.max_date - 8)), "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 8-14 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

# merging dataframes
unionOfCluster_sql = firstWeekCost.unionByName(secondWeekCost)

# window = Window.partitionBy(unionOfCluster_sql["organization_id"]).orderBy(unionOfCluster_sql['Total_Cost (USD)'].desc())
# rank = union.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)

w = Window.orderBy("organization_id", col("time_period").desc())
lagCol = lag(col("Total_Cost (USD)"), 1).over(w)

# #a dataframe that have values of last three weeks
totalClusterCost_sql = unionOfCluster_sql\
.withColumn("lm_total_cost", lagCol)\
.orderBy("time_period")\
.withColumn("Wow_change (USD)", round((col("Total_Cost (USD)") - col("lm_total_cost")), 2))\
.withColumn("WoW_change (%)", round(((col("Total_Cost (USD)") - col("lm_total_cost")) / col("lm_total_cost") * 100), 2))\
.where((col("time_period") != ("Last 8-14 days")))\
.orderBy(col("WoW_change (%)").desc())\
.select("time_period",
"organization_id",
"workspace_name",
"Total_Cost (USD)",
"lm_total_cost",
"WoW_change (%)",
"Wow_change (USD)")

window = Window.partitionBy(totalClusterCost_sql["organization_id"]).orderBy(totalClusterCost_sql['Total_Cost (USD)'].desc())
r3 = totalClusterCost_sql.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)


# Last three weeks data converted to pandas for visualization
totalCost_sql = r3.toPandas()



# Plotting dataframe view using plotly library
fig = px.bar(totalCost_sql,
x = "workspace_name",
y = "WoW_change (%)",
hover_data = ["organization_id",
"workspace_name", "WoW_change (%)", "Wow_change (USD)", "Total_Cost (USD)"],
color = "workspace_name",
title = "Weekly SQL cluster spend variation across all workspaces (top 5)")
fig.show()

# COMMAND ----------

# SQL Clusters

clusterNameSplit_sql = clusterstatefact\
.withColumn("date", clusterstatefact["timestamp_state_start"].cast("date"))\
.withColumn("organization_id", col("organization_id"))\
.withColumn('SqlEndpointId', json_tuple(col("custom_tags"), "SqlEndpointId"))\
.where((col('cluster_name') == 'SQLAnalytics'))

#Last week cost gets breakdown by workspace name
firstWeekCost = clusterNameSplit_sql\
.join(clusterstatefact_max, clusterNameSplit_sql.date > clusterstatefact_max.max_date - 31, "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 1-30 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

#Weekly cost gets breakdown by workspace name, for the week preceding penultimate week
secondWeekCost = clusterNameSplit_sql\
.join(clusterstatefact_max, ((clusterNameSplit_sql.date > clusterstatefact_max.max_date - 61)
& (clusterNameSplit_sql.date <= clusterstatefact_max.max_date - 31)), "inner")\
.groupBy(["organization_id",
"workspace_name"])\
.agg(round(sum(clusterstatefact["total_cost"]), 2).alias("Total_Cost (USD)"))\
.orderBy("Total_Cost (USD)")\
.select(lit("Last 31-60 days").alias("time_period"),
"organization_id",
"workspace_name",
"Total_Cost (USD)")

# merging dataframes
unionOfCluster_sql = firstWeekCost.unionByName(secondWeekCost)

# window = Window.partitionBy(unionOfCluster_sql["organization_id"]).orderBy(unionOfCluster_sql['Total_Cost (USD)'].desc())
# rank = union.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)

w = Window.orderBy("organization_id", col("time_period").desc())
lagCol = lag(col("Total_Cost (USD)"), 1).over(w)

# #a dataframe that have values of last three weeks
totalClusterCost_sql = unionOfCluster_sql\
.withColumn("lm_total_cost", lagCol)\
.orderBy("time_period")\
.withColumn("Wow_change (USD)", round((col("Total_Cost (USD)") - col("lm_total_cost")), 2))\
.withColumn("WoW_change (%)", round(((col("Total_Cost (USD)") - col("lm_total_cost")) / col("lm_total_cost") * 100), 2))\
.where((col("time_period") != ("Last 8-14 days")))\
.orderBy(col("WoW_change (%)").desc())\
.select("time_period",
"organization_id",
"workspace_name",
"Total_Cost (USD)",
"lm_total_cost",
"WoW_change (%)",
"Wow_change (USD)")

window = Window.partitionBy(totalClusterCost_sql["organization_id"]).orderBy(totalClusterCost_sql['Total_Cost (USD)'].desc())
r3 = totalClusterCost_sql.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)


# Last three weeks data converted to pandas for visualization
totalCost_sql = r3.toPandas()



# Plotting dataframe view using plotly library
fig = px.bar(totalCost_sql,
x = "workspace_name",
y = "WoW_change (%)",
hover_data = ["organization_id",
"workspace_name", "WoW_change (%)", "Wow_change (USD)", "Total_Cost (USD)"],
color = "workspace_name",
title = "Monthly SQL cluster spend variation across all workspaces")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC I would like to know when files are uploaded to DBFS
# MAGIC  and by whom in order to alert users when uploading restricted and
# MAGIC  confidential data.

# COMMAND ----------


