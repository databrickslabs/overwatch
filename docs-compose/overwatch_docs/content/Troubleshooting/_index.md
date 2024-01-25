---
title: "Troubleshooting"
date: 2022-12-13T13:49:40-05:00
---

This section is meant to help customers identify what might be causing an issue with a deployment or with the 
pipeline itself. 

Most of the issues customers face are related to the configuration of the different cloud artifacts required to run
Overwatch. We've created a notebook to help you troubleshoot:

**Readiness review notebook 0.8.x** [**HTML**](/assets/Troubleshooting/Readiness_Review_08x.html) | 
[**DBC**](/assets/Troubleshooting/Readiness_Review_08x.dbc)

To help our support team, please download, import, and execute the following notebook in the environment having the 
issue. If you're monitoring multiple workspaces from a single deployment, please execute the notebook on the 
workspace that is running Overwatch. The report has been anonymized to protect your privacy but still deliver the 
information necessary to troubleshoot the issue.

**Generic Anonymized Diagnostics Report** [**HTML**](/assets/Troubleshooting/GenericTicket_Diagnostics.html) |
[**DBC**](/assets/Troubleshooting/GenericTicket_Diagnostics.dbc)

Run the script and download the notebook in HTML format. (In the notebook: File -> Export -> HTML)

Once you have this HTML file you can either [open a GitHub ticket]((https://github.com/databrickslabs/overwatch/issues/new?assignees=&labels=user_question&template=overwatch-issue.md&title=), 
or contact your Databricks representative and attach the report to the email.


