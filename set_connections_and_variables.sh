#!/bin/bash

airflow connections add aws_credentials --conn-uri 'aws://***************'
airflow connections add redshift --conn-uri 'redshift://*****************:5439/dev'
airflow variables set s3_bucket udacity-dend
airflow variables set s3_prefix log_data