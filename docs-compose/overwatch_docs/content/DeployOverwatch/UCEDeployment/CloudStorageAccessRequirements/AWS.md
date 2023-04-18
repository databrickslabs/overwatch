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

### Instance Profile -

IAM Role – ROLE-NAME
Policy -POLiCY

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


Reasons for having delete policy -

* TEMP DIRECTORY:

Overwatch scrapes the data from the logs and from the api calls, as mentioned in its documentation. It requires a temporary location to write the intermediary datasets in the temporary location that is provided in the arguments while initializing the notebook/ job.
Since, overwatch’s goal is to be idempotent in nature, it needs to remove the intermediary locations since it is not required to have the data populated after its use. The cleansed datasets are loaded in the bronze, silver and the gold layers respectively and are ready to be used for dashboards.
Due to above mentioned scenarios, it needs to delete/ vacuum the datasets from the temporary locations. Hence, overwatch issues a remove command at the defined location which is set by the user/ admin “/tmp/overwatch/templocation”.

* DELTA VACUUM:

The delta tables by concept needs to be vacuumed and optimized which cleans up the file systems relative to the table. However, before these actions, new files are written with the data written in an optimized way thereby reducing the number of files in the directory. None of the data is impacted with this operation. These will be impacting in the Bronze, Silver and Gold data paths relative to overwatch storage locations. Overwatch requires the respective permissions to do so.
