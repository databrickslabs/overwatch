---
title: "As A JAR (Legacy)"
date: 2022-12-13T16:09:07-05:00
weight: 5
---

{{% notice warning%}}
As of version 0.7.1.0 Overwatch will begin sunsetting this deployment method. Please reference
[Running A Job As A Jar]({{%relref "DeployOverwatch/RunningOverwatch/JAR.md"%}}) for the updated method for
deploying Overwatch. This method will likely be deprecated in Overwatch version 0.8 and no longer be supported in 0.9.
{{% /notice %}}

The new deployment method provides support for a "single-workspace deployment" or "multi-workspace deployment" where
a single Overwatch job is configured and loads data from all workspaces. More importantly, there are at most 3 
parameters that can be passed in much more simplistically. More details available in
[Running A Job As A JAR]({{%relref "DeployOverwatch/RunningOverwatch/JAR.md"%}})

## Executing Overwatch As A JAR Job (Legacy)
Executing Overwatch as a JAR is the same as via a notebook. The only difference is how the configs are passed in. 
The tricky party about the main class is that the parameters must be passed in as a json array of
escaped strings. 

The [Jump Start Notebooks]({{%relref "DeployOverwatch/RunningOverwatch/NotebookLegacy"%}}/#jump-start-notebooks) are 
there to help you configure Overwatch correctly with validations in place and then output this escaped json string 
for you; this is done on the line
```scala
// use this to run Overwatch as a job
JsonUtils.objToJson(params).escapedString
```

### Running A Specific Pipeline
Notice that the escaped Overwatch Json config goes inside the array of strings parameter config. If you only want to 
run the Silver pipeline simply add a single string in front of the json escaped string as the first parameter.
```json
["silver", "<overwatch_escaped_params>"]
```
The full list of optional parameters is below. If you do not specify a specific pipeline, all will be executed.
```json
["<overwatch_escaped_params>"]
["bronze", "<overwatch_escaped_params>"]
["silver", "<overwatch_escaped_params>"]
["gold", "<overwatch_escaped_params>"]
```

{{% notice info %}}
**If a single argument** passed to main class: Arg(0) is Overwatch Parameters json string (escaped if sent through API).
**If TWO arguments** are passed to main class: Arg(0) must be 'bronze', 'silver', or 'gold' corresponding to the
Overwatch Pipeline layer you want to be run. Arg(1) will be the Overwatch Parameters json string.
{{% /notice %}}

![newUIJarSetup](/images/GettingStarted/0601JobSetupExample.png)