#!/usr/bin/env zsh

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account)
AWS_REGION=$(aws configure get region)

aws cloudformation deploy --template-file ops/stack.yaml --stack-name data-engineering-stack --capabilities CAPABILITY_IAM
