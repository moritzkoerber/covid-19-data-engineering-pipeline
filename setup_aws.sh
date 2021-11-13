#!/usr/bin/env zsh

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account)
AWS_REGION=$(aws configure get region)

aws cloudformation deploy --template-file ops/stack.yaml --stack-nam data-engineering-stack