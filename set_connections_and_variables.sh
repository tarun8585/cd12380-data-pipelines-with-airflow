#!/bin/bash

airflow connections add aws_credentials --conn-uri 'aws://AKIA264YYGFTLOL6E5AL:MI%2FaL9wr2fou7qwJDojWq8MeHmCUjbxhpDElxMmi@'
airflow connections add redshift --conn-uri 'redshift://awsuser:R3dsh1ft@default-workgroup.753550242150.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
airflow variables set s3_bucket arun-thiru
airflow variables set s3_prefix data-pipelines