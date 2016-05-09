# csv-receiver
A small Lambda app that receives a CSV and pushes its row content as JSON messages to SNS.

Once deployed to your AWS account, you can pass CSVs through the system through any of these channels:

1. HTTP POST the CSV contents to the endpoint listed in the stack outputs, followed by the stream name:

```
curl -X POST -H "Content-Type: text/plain" -d "foo,bar
11,22
33,44" "https://{some-unique-id}.execute-api.us-east-1.amazonaws.com/prod/{stream-name}"
```

2. Upload a CSV to S3 and [notify the Lambda function](#setting-up-s3-notifications)


## Deployment (users)
To deploy this stack to your AWS account, simply do the following:

1. Create a S3 bucket where the CSVs can be saved
2. Create a CloudFormation stack using the [csv-receiver-deploy.template](https://s3.amazonaws.com/com.gilt.public.backoffice/cloudformation_templates/csv-receiver-deploy.template)

That's it!


## Setting up S3 Notifications
You should set up notifications from your target CSV bucket to call this Lambda function. Since this will
be very specific to your setup, the CloudFormation template doesn't cover this. But there is a [shell command
file](bin/setup-s3-notification) included that you can use to add notifications from S3 to the function
created by the CloudFormation stack.


## Alerts
The CloudFormation template automatically creates these metrics:

1. Errors
2. Messages published

You will need to set up alerts based on these metrics to notify through your preferred channel. Errors should be
0, so any non-zero count should trigger an alert. Messages published will depend on your workload; you can also
duplicate the metric for stream-specific counts.


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