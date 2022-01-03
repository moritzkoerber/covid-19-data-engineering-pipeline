#!/usr/bin/env zsh

aws s3 sync . s3://data-pipeline-s3-bucket-production/expectations --exclude sync_expectations.sh --exclude .DS_Store
