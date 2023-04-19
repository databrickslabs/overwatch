---
title: "AWS"
date: 2023-04-18T11:28:39-05:00
weight: 1
---
**Under Construction** -- we will be improving these docs shortly

### AWS IAM/Policy required to set up for Storage Credentials
```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:GetLifecycleConfiguration",
        "s3:PutLifecycleConfiguration"
      ],
      "Resource": [
        "arn:aws:s3:::<BUCKET-NAME>/*",
        "arn:aws:s3:::< BUCKET-NAME> "
      ],
      "Effect": "Allow"
    },
    {
      "Action": [
        "kms:Decrypt",
        "kms:Encrypt",
        "kms:GenerateDataKey*"
      ],
      "Resource": [
        "arn:aws:kms:<KMS_KEY>"
      ],
      "Effect": "Allow"
    },
    {
      "Action": [
        "sts:AssumeRole"
      ],
      "Resource": [
        "arn:aws:iam::<AWS-ACCOUNT-ID>:role/<THIS-IAM-ROLE>"
      ],
      "Effect": "Allow"
    }
  ]
}
```

### Trust Relation
```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL" //DO NOT CHANGE
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "e6e8162c-a42f-43a0-af86-312058795a14"
        }
      }
    }
  ]
}
```

### Instance Profile - IAM Role / Policy

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PermitSelectedBucketsList",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:PutObjectAcl",
        "s3:GetBucketNotification",
        "s3:PutBucketNotification"
      ],
      "Resource": [
        "arn:aws:s3:::<BUCKET-NAME>/*",
        "arn:aws:s3:::< BUCKET-NAME> "
      ]
    },
    {
      "Action": [
        "kms:Decrypt",
        "kms:Encrypt",
        "kms:GenerateDataKey*"
      ],
      "Resource": [
        "arn:aws:kms:<KMS_KEY>"
      ],
      "Effect": "Allow"
    },
    {
      "Sid": "DenyAuditLogsBucketCRUD",
      "Effect": "Deny",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::<AUDIT-LOG-BUCKET-NAME>/*"
      ]
    }
  ]
}
```


**Why Is Delete Required In The Policy**

* TEMP DIRECTORY
  * Overwatch scrapes the data from the logs and from the api calls, as mentioned in its documentation. 
  It requires a temporary location to write intermediate datasets.
* DELTA VACUUM
  * The delta tables need to be vacuumed and optimized to maintain efficiency and optimize the tables for downstream use
  Please visit the Databricks docs for more details on 
  [Delta Optimize](https://docs.databricks.com/sql/language-manual/delta-optimize.html) & 
  [Delta Vacuum](https://docs.databricks.com/sql/language-manual/delta-vacuum.html).
