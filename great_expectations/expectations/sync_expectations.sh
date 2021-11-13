#!/usr/bin/env zsh

env=$1
aws s3 cp exp_suite.json s3://data-pipeline-s3-bucket-${env}/expectations/exp_suite.json
