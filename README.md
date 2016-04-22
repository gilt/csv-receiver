# csv-receiver
A small Lambda app that receives a CSV and pushes its row content as messages to SNS

## Setting up S3 Notifications
You should set up notifications from your target CSV bucket to call this Lambda function. Since this will
be very specific to your setup, the CloudFormation template doesn't cover this. But there is a [shell command
file](bin/setup-s3-notification) included that you can use to add notifications from S3 to the function
created by the CloudFormation stack.

## Deployment (contributors)
After making functionality changes, please do the following:

1. Upload this zipped repo to the com.gilt.public.backoffice/lambda_functions bucket. To produce the .zip file:

   ```
     rm -rf node_modules
     npm install --production
     zip -r csv-receiver.zip . -x *.git* -x *csv-receiver.zip* -x test/\* -x cloud_formation/\* -x *aws-sdk*
   ```

   Unfortunately we can't use the Github .zip file directly, because it zips the code into a subdirectory named after
   the repo; AWS Lambda then can't find the .js file containing the helper functions because it is not on the top-level.

2. Upload the edited create_cloudformation/csv-receiver-deploy.template to com.gilt.public.backoffice/cloudformation_templates


## License
Copyright 2016 Gilt Groupe, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0