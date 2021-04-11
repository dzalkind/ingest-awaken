# Steps

## Prereqs:
* Need docker
* Need sam cli
* Need aws cli
* Setup aws credentials
* Need to create ECR repo. You can do that from your AWS management console if you
navigate to the Elastic Container Registry service.

https://us-west-2.console.aws.amazon.com/ecr/repositories?region=us-west-2

## Deploy

### 1) Edit samconfig.toml file to set your parameters.
Make sure to set the stack name the same as your project name to avoid confusion.

### 2) sam build
To build your lambda image, run this from your project dir:

```bash
sam build
```
This will create an .aws-sam folder.  

Under .aws-sam/build folder it creates a template.yaml.  This is the conversion
of your sam template into the full cloud formation template syntax.

It will also build a docker image for you.  The name of the image is the name
of your lambda function in your template.yaml except it converts to all lower case.

```bash
e.g., tsdatpipeline
```

### 3) sam deploy

This is what the console looks like after deploy

```bash

        Deploying with following values
        ===============================
        Stack name                   : tsdat-sam-test
        Region                       : us-west-2
        Confirm changeset            : True
        Deployment image repository  :
                                       {
                                           "TsdatPipeline": "332883119153.dkr.ecr.us-west-2.amazonaws.com/tsdat-sam-test"
                                       }
        Deployment s3 bucket         : aws-sam-cli-managed-default-samclisourcebucket-rq99cgy93a46
        Capabilities                 : ["CAPABILITY_IAM"]
        Parameter overrides          : {"SourceBucket": "tsdat-raw", "DestinationBucket": "tsdat-processed"}
        Signing Profiles             : {}

Initiating deployment
=====================
Uploading to tsdat-sam-test/f10306131c25126c33afc330ec476f89.template  1662 / 1662  (100.00%)

Waiting for changeset to be created..

CloudFormation stack changeset
---------------------------------------------------------------------------------------------------------------------------------------------------------
Operation                              LogicalResourceId                      ResourceType                           Replacement
---------------------------------------------------------------------------------------------------------------------------------------------------------
+ Add                                  DestinationS3Bucket                    AWS::S3::Bucket                        N/A
+ Add                                  SourceS3Bucket                         AWS::S3::Bucket                        N/A
+ Add                                  TsdatPipelineFileUploadedPermission    AWS::Lambda::Permission                N/A
+ Add                                  TsdatPipelineRole                      AWS::IAM::Role                         N/A
+ Add                                  TsdatPipeline                          AWS::Lambda::Function                  N/A
- Delete                               HelloWorldFunction                     AWS::Lambda::Function                  N/A
---------------------------------------------------------------------------------------------------------------------------------------------------------

Changeset created successfully. arn:aws:cloudformation:us-west-2:332883119153:changeSet/samcli-deploy1617843414/a54d16c0-def4-4ab1-a1e8-cce931c7a413

```

Looks like SAM may create bucket with public permissions.  How do we change that to private?
How do we set up bucket versioning on the created bucket?

Lambda function will be create with the name specified in the template.yaml:
FunctionName: tsdat-pipeline

### 4) Run the pipeline
From the AWS S3 console, upload a file to your pipeline's source bucket.

### 5) Check the log to see that it ran
See it from the cloud watch console:
https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/$252Faws$252Flambda$252Ftsdat-pipeline

You can also see it from the command line
```bash
tsdat-sam-test$  sam logs -n tsdat-pipeline
```


# Deleting your stack
Note that AWS won't let you delete your bucket if there is data in there.  From the
S3 console you can empty the bucket first to remove all files.

```bash
aws cloudformation delete-stack --stack-name tsdat-sam-test
```

If delete fails:
https://aws.amazon.com/premiumsupport/knowledge-center/cloudformation-stack-delete-failed/