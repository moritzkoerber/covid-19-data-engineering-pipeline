This repo is my playground to try out various data engineering stuff. The used services/tools/design is not always the best choice or sometimes unnecessary cumbersome – this just reflects me trying to explore different things. At the moment, the pipeline processes Covid-19 data as follows:
![aws](https://user-images.githubusercontent.com/25953031/208241520-4e09d09f-d2c4-44cc-b9ed-b05bcbd5b1a6.png)
All infrastructure is templated in AWS CloudFormation or AWS CDK. All steps feature an alarm on failure. The stack can be deployed via Github Actions.
