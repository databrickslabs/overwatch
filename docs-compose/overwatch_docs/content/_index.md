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
* **Realtime** -- Good for monitoring / alerting
    * Utilizes standard integrated drop-wizard and collectd data publications methods through Graphite or Prometheus.
      See [Spark Monitoring](https://spark.apache.org/docs/latest/monitoring.html#monitoring-and-instrumentation) for more information
    * Init script to be published ASAP but requires client-side architecture set up. Example client-side 
    architecture [available below](#realtime--monitoring--alerting).

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
Overwatch is often integrated with real-time solutions to enhance the data provided as raw Spark Metrics. For example, 
you may be monitoring jobs in real-time but want job_names instead of job_ids, Overwatch's slow-changing dimensions 
can enhance this.

Real-time monitoring usually comes from at least two different sources:
* spark via [DropWizard](https://spark.apache.org/docs/latest/monitoring.html#metrics) (a default spark monitoring platform)
* machine metrics via [collectd](https://collectd.org/) or some native cloud solutions such as CloudWatch or LogAnalytics.

These real-time metrics can be captured as quickly as 5s intervals but it's critical to note that proper 
historization is a must for higher intervals; furthermore, it's critical to configure the metrics at the "correct" 
interval for your business needs. In other words, it can get quickly get expensive to load all metrics at 5s 
intervals. All of these details are likely common knowledge to a team that manages a time-series database (TSDB).

Below is a scalable, reference architecture using Graphite and Grafana to capture these metrics. Overwatch 
creates several daily JSON time-series compatible exports to Grafana JSON that provide slow-changing dimensional 
lookups between real-time keys and dimensional values through joins for enhanced dashboards.

Below is a link to a notebook offering samples for integrating Spark and machine metrics to some real-time 
infrastructure endpoint. The examples in this notebook offer examples to Prometheus, Graphite, and Log Analytics 
for Spark Metrics and collectD for machine metrics. **Critical** this is just a sample for review, this 
notebook is intended as a reference to guide you to creating your own implementation, you must 
create a script to be valid for your requirements and capture the right metrics and the right intervals for 
the namespaces from which you wish to capture metrics.

Realtime Getting Started Reference Notebook [**HTML**](/assets/_index/realtime_helpers.html) | [**DBC**](/assets/_index/realtime_helpers.dbc)

{{% notice note %}}
The realtime reference architecture has been validated but 1-click delivery has not yet been enabled. The 
time-series database / infrastructure setup is the responsibility of the customer; Databricks can assist with 
integrating the spark metrics delivery with customer infrastructure but Databricks cannot offer much depth for 
standing up / configuring the real-time infrastructure itself.
{{% /notice %}}

#### Example Realtime Architecture  
![RealTime Diagram](/images/_index/Realtime_example_architecture.png)

#### Example Realtime Dashboards
Simple Spark Dashboard | IO Dashboard | Advanced Dashboard
:-------------------------:|:-------------------------:|:-------------------------:
![RealTime Dasboard1](/images/_index/spark_dashboard1.png) | ![RealTime Dasboard2](/images/_index/spark_dashboard2.png) | ![RealTime Dasboard3](/images/_index/spark_dashboard3.png)