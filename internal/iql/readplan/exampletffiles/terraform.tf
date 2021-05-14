{
  "version": 4,
  "terraform_version": "0.12.23",
  "serial": 165,
  "lineage": "30dd0f63-20d6-792f-7af9-1ae7cb3202b4",
  "outputs": {},
  "resources": [
    {
      "mode": "data",
      "type": "aws_iam_policy_document",
      "name": "sns_topic",
      "provider": "provider.aws",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "id": "4191451652",
            "json": "{\n  \"Version\": \"2012-10-17\",\n  \"Statement\": [\n    {\n      \"Sid\": \"\",\n      \"Effect\": \"Allow\",\n      \"Action\": \"s3:ListAllMyBuckets\",\n      \"Resource\": \"arn:aws:s3:::*\"\n    },\n    {\n      \"Sid\": \"\",\n      \"Effect\": \"Allow\",\n      \"Action\": [\n        \"s3:PutObjectAcl\",\n        \"s3:PutObject\",\n        \"s3:ListBucket\"\n      ],\n      \"Resource\": [\n        \"arn:aws:s3:::ellipsis-snowpipe-stage/*\",\n        \"arn:aws:s3:::ellipsis-snowpipe-stage\"\n      ]\n    }\n  ]\n}",
            "override_json": null,
            "policy_id": null,
            "source_json": null,
            "statement": [
              {
                "actions": [
                  "s3:ListAllMyBuckets"
                ],
                "condition": [],
                "effect": "Allow",
                "not_actions": [],
                "not_principals": [],
                "not_resources": [],
                "principals": [],
                "resources": [
                  "arn:aws:s3:::*"
                ],
                "sid": ""
              },
              {
                "actions": [
                  "s3:ListBucket",
                  "s3:PutObject",
                  "s3:PutObjectAcl"
                ],
                "condition": [],
                "effect": "Allow",
                "not_actions": [],
                "not_principals": [],
                "not_resources": [],
                "principals": [],
                "resources": [
                  "arn:aws:s3:::ellipsis-snowpipe-stage",
                  "arn:aws:s3:::ellipsis-snowpipe-stage/*"
                ],
                "sid": ""
              }
            ],
            "version": "2012-10-17"
          }
        }
      ]
    },
    {
      "mode": "data",
      "type": "aws_iam_policy_document",
      "name": "sftp_instance_write_access_policy_doc",
      "provider": "provider.aws",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "id": "3595197192",
            "json": "{\n  \"Version\": \"2012-10-17\",\n  \"Statement\": [\n    {\n      \"Sid\": \"\",\n      \"Effect\": \"Allow\",\n      \"Action\": [\n        \"s3:PutObjectAcl\",\n        \"s3:PutObject\"\n      ],\n      \"Resource\": \"arn:aws:s3:::ellipsis-snowpipe-stage/*\",\n      \"Principal\": {\n        \"AWS\": \"arn:aws:iam::692401932224:role/sftp_instance_role\"\n      }\n    },\n    {\n      \"Sid\": \"\",\n      \"Effect\": \"Allow\",\n      \"Action\": [\n        \"s3:PutObject\",\n        \"s3:GetObjectVersion\",\n        \"s3:GetObject\",\n        \"s3:DeleteObjectVersion\",\n        \"s3:DeleteObject\"\n      ],\n      \"Resource\": \"arn:aws:s3:::ellipsis-snowpipe-stage/*\",\n      \"Principal\": {\n        \"AWS\": \"arn:aws:iam::692401932224:user/snowflake_stage_user\"\n      }\n    },\n    {\n      \"Sid\": \"\",\n      \"Effect\": \"Allow\",\n      \"Action\": \"s3:ListBucket\",\n      \"Resource\": \"arn:aws:s3:::ellipsis-snowpipe-stage\",\n      \"Principal\": {\n        \"AWS\": \"arn:aws:iam::692401932224:user/snowflake_stage_user\"\n      }\n    }\n  ]\n}",
            "override_json": null,
            "policy_id": null,
            "source_json": null,
            "statement": [
              {
                "actions": [
                  "s3:PutObject",
                  "s3:PutObjectAcl"
                ],
                "condition": [],
                "effect": "Allow",
                "not_actions": [],
                "not_principals": [],
                "not_resources": [],
                "principals": [
                  {
                    "identifiers": [
                      "arn:aws:iam::692401932224:role/sftp_instance_role"
                    ],
                    "type": "AWS"
                  }
                ],
                "resources": [
                  "arn:aws:s3:::ellipsis-snowpipe-stage/*"
                ],
                "sid": ""
              },
              {
                "actions": [
                  "s3:DeleteObject",
                  "s3:DeleteObjectVersion",
                  "s3:GetObject",
                  "s3:GetObjectVersion",
                  "s3:PutObject"
                ],
                "condition": [],
                "effect": "Allow",
                "not_actions": [],
                "not_principals": [],
                "not_resources": [],
                "principals": [
                  {
                    "identifiers": [
                      "arn:aws:iam::692401932224:user/snowflake_stage_user"
                    ],
                    "type": "AWS"
                  }
                ],
                "resources": [
                  "arn:aws:s3:::ellipsis-snowpipe-stage/*"
                ],
                "sid": ""
              },
              {
                "actions": [
                  "s3:ListBucket"
                ],
                "condition": [],
                "effect": "Allow",
                "not_actions": [],
                "not_principals": [],
                "not_resources": [],
                "principals": [
                  {
                    "identifiers": [
                      "arn:aws:iam::692401932224:user/snowflake_stage_user"
                    ],
                    "type": "AWS"
                  }
                ],
                "resources": [
                  "arn:aws:s3:::ellipsis-snowpipe-stage"
                ],
                "sid": ""
              }
            ],
            "version": "2012-10-17"
          }
        }
      ]
    },
    {
      "mode": "data",
      "type": "aws_iam_role",
      "name": "sftp_instance_role",
      "provider": "provider.aws",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "arn": "arn:aws:iam::692401932224:role/sftp_instance_role",
            "assume_role_policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"\",\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"ec2.amazonaws.com\"},\"Action\":\"sts:AssumeRole\"}]}",
            "assume_role_policy_document": null,
            "create_date": "2018-11-23T04:59:33Z",
            "description": "",
            "id": "sftp_instance_role",
            "max_session_duration": 3600,
            "name": "sftp_instance_role",
            "path": "/",
            "permissions_boundary": "",
            "role_id": null,
            "role_name": null,
            "unique_id": "AROAIMNTZCYW6O44KKL7C"
          }
        }
      ]
    },
    {
      "mode": "data",
      "type": "aws_iam_user",
      "name": "snowflake_stage_user",
      "provider": "provider.aws",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "arn": "arn:aws:iam::692401932224:user/snowflake_stage_user",
            "id": "AIDAIQCYDEQTC2QUMHP44",
            "path": "/",
            "permissions_boundary": "",
            "user_id": "AIDAIQCYDEQTC2QUMHP44",
            "user_name": "snowflake_stage_user"
          }
        }
      ]
    },
    {
      "mode": "data",
      "type": "aws_kms_alias",
      "name": "ext_stage_key",
      "provider": "provider.aws",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "arn": "arn:aws:kms:ap-southeast-2:692401932224:alias/snowflake_stage",
            "id": "2020-03-15 10:21:10.3349487 +0000 UTC",
            "name": "alias/snowflake_stage",
            "target_key_arn": "arn:aws:kms:ap-southeast-2:692401932224:key/483c22ad-7260-4432-83ff-dfaa77966c5d",
            "target_key_id": "483c22ad-7260-4432-83ff-dfaa77966c5d"
          }
        }
      ]
    },
    {
      "mode": "managed",
      "type": "aws_iam_role_policy",
      "name": "sftp_instance_role_policy",
      "provider": "provider.aws",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "id": "sftp_instance_role:sftp_instance_role_policy_v2",
            "name": "sftp_instance_role_policy_v2",
            "name_prefix": null,
            "policy": "{\n  \"Version\": \"2012-10-17\",\n  \"Statement\": [\n    {\n      \"Sid\": \"\",\n      \"Effect\": \"Allow\",\n      \"Action\": \"s3:ListAllMyBuckets\",\n      \"Resource\": \"arn:aws:s3:::*\"\n    },\n    {\n      \"Sid\": \"\",\n      \"Effect\": \"Allow\",\n      \"Action\": [\n        \"s3:PutObjectAcl\",\n        \"s3:PutObject\",\n        \"s3:ListBucket\"\n      ],\n      \"Resource\": [\n        \"arn:aws:s3:::ellipsis-snowpipe-stage/*\",\n        \"arn:aws:s3:::ellipsis-snowpipe-stage\"\n      ]\n    }\n  ]\n}",
            "role": "sftp_instance_role"
          },
          "private": "bnVsbA==",
          "dependencies": [
            "aws_s3_bucket.ellipsis_ext_stage"
          ]
        }
      ]
    },
    {
      "mode": "managed",
      "type": "aws_s3_bucket",
      "name": "ellipsis_ext_stage",
      "provider": "provider.aws",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "acceleration_status": "",
            "acl": "private",
            "arn": "arn:aws:s3:::ellipsis-snowpipe-stage",
            "bucket": "ellipsis-snowpipe-stage",
            "bucket_domain_name": "ellipsis-snowpipe-stage.s3.amazonaws.com",
            "bucket_prefix": null,
            "bucket_regional_domain_name": "ellipsis-snowpipe-stage.s3.ap-southeast-2.amazonaws.com",
            "cors_rule": [],
            "force_destroy": false,
            "grant": [],
            "hosted_zone_id": "Z1WCIGYICN2BYD",
            "id": "ellipsis-snowpipe-stage",
            "lifecycle_rule": [],
            "logging": [],
            "object_lock_configuration": [],
            "policy": null,
            "region": "ap-southeast-2",
            "replication_configuration": [],
            "request_payer": "BucketOwner",
            "server_side_encryption_configuration": [
              {
                "rule": [
                  {
                    "apply_server_side_encryption_by_default": [
                      {
                        "kms_master_key_id": "arn:aws:kms:ap-southeast-2:692401932224:alias/snowflake_stage",
                        "sse_algorithm": "aws:kms"
                      }
                    ]
                  }
                ]
              }
            ],
            "tags": {
              "DateModified": "2020-03-15T21:20:53",
              "Origin": "Terraform",
              "Project": "Ellipsis Cloud"
            },
            "versioning": [
              {
                "enabled": false,
                "mfa_delete": false
              }
            ],
            "website": [],
            "website_domain": null,
            "website_endpoint": null
          },
          "private": "bnVsbA=="
        }
      ]
    },
    {
      "mode": "managed",
      "type": "aws_s3_bucket_notification",
      "name": "bucket_notification_sns",
      "provider": "provider.aws",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "bucket": "ellipsis-snowpipe-stage",
            "id": "ellipsis-snowpipe-stage",
            "lambda_function": [],
            "queue": [],
            "topic": [
              {
                "events": [
                  "s3:ObjectCreated:*"
                ],
                "filter_prefix": "",
                "filter_suffix": ".log",
                "id": "tf-s3-topic-20200315101419291300000001",
                "topic_arn": "arn:aws:sns:ap-southeast-2:692401932224:snowpipe_sns_topic"
              }
            ]
          },
          "private": "bnVsbA==",
          "dependencies": [
            "aws_s3_bucket.ellipsis_ext_stage",
            "aws_sns_topic.sns_topic"
          ]
        }
      ]
    },
    {
      "mode": "managed",
      "type": "aws_s3_bucket_policy",
      "name": "sftp_instance_write_access",
      "provider": "provider.aws",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "bucket": "ellipsis-snowpipe-stage",
            "id": "ellipsis-snowpipe-stage",
            "policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::692401932224:role/sftp_instance_role\"},\"Action\":[\"s3:PutObjectAcl\",\"s3:PutObject\"],\"Resource\":\"arn:aws:s3:::ellipsis-snowpipe-stage/*\"},{\"Sid\":\"\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::692401932224:user/snowflake_stage_user\"},\"Action\":[\"s3:PutObject\",\"s3:GetObjectVersion\",\"s3:GetObject\",\"s3:DeleteObjectVersion\",\"s3:DeleteObject\"],\"Resource\":\"arn:aws:s3:::ellipsis-snowpipe-stage/*\"},{\"Sid\":\"\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::692401932224:user/snowflake_stage_user\"},\"Action\":\"s3:ListBucket\",\"Resource\":\"arn:aws:s3:::ellipsis-snowpipe-stage\"}]}"
          },
          "private": "bnVsbA==",
          "dependencies": [
            "aws_s3_bucket.ellipsis_ext_stage"
          ]
        }
      ]
    },
    {
      "mode": "managed",
      "type": "aws_s3_bucket_public_access_block",
      "name": "ellipsis_ext_stage",
      "provider": "provider.aws",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "block_public_acls": true,
            "block_public_policy": true,
            "bucket": "ellipsis-snowpipe-stage",
            "id": "ellipsis-snowpipe-stage",
            "ignore_public_acls": false,
            "restrict_public_buckets": false
          },
          "private": "bnVsbA==",
          "dependencies": [
            "aws_s3_bucket.ellipsis_ext_stage"
          ]
        }
      ]
    },
    {
      "mode": "managed",
      "type": "aws_sns_topic",
      "name": "sns_topic",
      "provider": "provider.aws",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "application_failure_feedback_role_arn": "",
            "application_success_feedback_role_arn": "",
            "application_success_feedback_sample_rate": 0,
            "arn": "arn:aws:sns:ap-southeast-2:692401932224:snowpipe_sns_topic",
            "delivery_policy": "",
            "display_name": "",
            "http_failure_feedback_role_arn": "",
            "http_success_feedback_role_arn": "",
            "http_success_feedback_sample_rate": 0,
            "id": "arn:aws:sns:ap-southeast-2:692401932224:snowpipe_sns_topic",
            "kms_master_key_id": "",
            "lambda_failure_feedback_role_arn": "",
            "lambda_success_feedback_role_arn": "",
            "lambda_success_feedback_sample_rate": 0,
            "name": "snowpipe_sns_topic",
            "name_prefix": null,
            "policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"1\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"*\"},\"Action\":\"SNS:Publish\",\"Resource\":\"arn:aws:sns:*:*:snowpipe_sns_topic\",\"Condition\":{\"ArnLike\":{\"aws:SourceArn\":\"arn:aws:s3:::ellipsis-snowpipe-stage\"}}},{\"Sid\":\"2\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::692401932224:user/snowflake_stage_user\"},\"Action\":\"sns:Subscribe\",\"Resource\":\"arn:aws:sns:*:*:snowpipe_sns_topic\"}]}",
            "sqs_failure_feedback_role_arn": "",
            "sqs_success_feedback_role_arn": "",
            "sqs_success_feedback_sample_rate": 0,
            "tags": {}
          },
          "private": "bnVsbA==",
          "dependencies": [
            "aws_s3_bucket.ellipsis_ext_stage"
          ]
        }
      ]
    },
    {
      "mode": "managed",
      "type": "null_resource",
      "name": "sns_subscribe",
      "provider": "provider.null",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
            "id": "2843948734943193684",
            "triggers": {
              "sns_topic_arn": "arn:aws:sns:ap-southeast-2:692401932224:snowpipe_sns_topic"
            }
          },
          "private": "bnVsbA==",
          "dependencies": [
            "aws_sns_topic.sns_topic"
          ]
        }
      ]
    }
  ]
}
