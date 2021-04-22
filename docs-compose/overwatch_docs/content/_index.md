---
title: "Welcome To OverWatch"
---

## Project Overview

Overwatch was built to enable Databricks' customers, employees, and partners to quickly / easily understand 
operations within Databricks deployments. As enterprise adoption increases there's an ever-growing need for strong
governance. Overwatch means to enable users to quickly answer questions and then drill down to make effective 
operational changes. Common examples of operational activities Overwatch assists with are: 
* Cost tracking by various dimensions
* Governance at various dimensions
* Optimization Opportunities
* What If Experiments

## How Much Does It Cost
Overwatch is currently a Databricks Labs project and has **no direct cost** associated with it other than the costs
incurred with running the daily jobs. Audit logging is required and as such a 
[**Azure Databricks Premium SKU**](https://databricks.com/product/azure-pricing) **OR**
the equivalent [**AWS Premium Plan**](https://databricks.com/product/aws-pricing) or above.
As a reference, a cost analysis was performed at a large Databricks customer.
This customer had >1000 named users with >400 daily active users with a contract price with Databricks over $2MM/year. 
Overwatch costs them approximately $8 USD / day to run which includes storage, compute, and DBUs. $8 / day == 
approximately $3,000 / year or **0.15% of Databricks contract price**. This is just a single reference customer but 
cost monitoring for your Overwatch job should be the same as any other job you run. At the time of this writing 
Overwatch has only been deployed as a pilot and has already proven several million dollars in value. The savings 
customers found through these efficiency improvements allowed them to accelerate their backlogs, permanently lower the 
total cost of ownership, and even take on additional exploratory efforts.

## How It Works
Overwatch has two primary modes, Historical & Realtime
* **Historical** -- Good for identifying trends, outliers, issues, challenges
* **Realtime** (coming 2021) -- Good for monitoring / alerting

### Data Process Flow
![OverwatchProcessFlow](/images/_index/ProcessFlow.png)

### Batch / Historical
Overwatch amalgamates and unifies all the logs produced by Spark and Databricks via a periodic job run (typically 
1x/day). The Overwatch job then enriches this data through various API calls to the Databricks platform and, 
in some cases, the cloud provider. Finally, all the data is pulled together into a data model 
that enables simplified queries for powerful insights and governance. Below are a few example reports produced
by Overwatch. The batch / historical data is typically used to identify the proper boundaries that can later be used
to set up monitoring / alerts in the real-time modules.

Hot Notebooks | Heavy User Breakdown
:-------------------------:|:-------------------------:
![BatchReport1](/images/_index/Hot_Notebooks.png) | ![BatchReport2](/images/_index/outlierUserDetail.png)

### Realtime / Monitoring / Alerting
Overwatch does have a configurable, near-realtime component that allows users to retrieve metrics at a configurable
frequency (i.e. 5s/120s/etc). This is enabled through [DropWizard]() (a default spark monitoring platform) and a 
time-series database (TSDB) called "whisper" which is wrapped by Graphite. Graphite offers many exposed APIs upon which 
any front-end TSDB reporting tool can be used, such as Grafana. Graphite and Grafana are used by default as they do not
require a license for basic use and Grafana is very common in the industry. Very powerful monitoring dashboards
along with configured alerting can be enabled through Grafana.

{{% notice note %}}
The realtime modules are set to be released as part of Overwatch as soon as possible. The Dev team is targeting
Q1 2021 for release but this is not a guarantee. Realtime is not being released as part of v1.0 as the cloud 
infrastructure has not been enabled via one-click deploy just yet. As such, there's no reason a user cannot enable
this but it's outside the scope of the Overwatch project at this time. Below is an example realtime architecture
diagram to assist in the event you want to tackle this before it's packaged with Overwatch.
{{% /notice %}}

#### Example Realtime Architecture  
![RealTime Diagram](/images/_index/Realtime_example_architecture.png)

#### Example Realtime Dashboards
Simple Spark Dashboard | IO Dashboard | Advanced Dashboard
:-------------------------:|:-------------------------:|:-------------------------:
![RealTime Dasboard1](/images/_index/spark_dashboard1.png) | ![RealTime Dasboard2](/images/_index/spark_dashboard2.png) | ![RealTime Dasboard3](/images/_index/spark_dashboard3.png)