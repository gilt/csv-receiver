var Promise = require('bluebird'),
    AWS = require('aws-sdk'),
    conf = Promise.promisifyAll(require('aws-lambda-config')),
    csvHelper = require('../lib/csvHelper'),
    csvMultiThreader = require('./csvMultiThreader'),
    csvProcessor = require('./csvProcessor'),
    s3 = Promise.promisifyAll(new AWS.S3()),
    sha1 = require('sha1');

// Receives a CSV via HTTP POST through API Gateway
exports.handle = function(event, context) {
  return conf.getConfigAsync(context)
  .then(function(config) {
    return Promise.try(function() {
      if (event.Records && event.Records[0].Sns) event = JSON.parse(event.Records[0].Sns.Message);
      if (event.Body) {
        // Process the Body of the CSV
        if (!config.Bucket) throw new Error("Missing S3 bucket for stream");
        if (!event.StreamName) throw new Error("Missing StreamName for processing csv body");
        // Save the raw file
        return s3.putObjectAsync({
          Bucket: config.Bucket,
          Key: "raw/" + event.StreamName + "/" + sha1(event.Body) + ".txt",
          Body: event.Body
        })
        .then(function() {
          event.Metadata = event.Metadata || {};
          event.Metadata.MultiThreading = true;
          var csv = new csvHelper.Csv();
          return csv.load(event.Body, null, config.Delimiter)
          .then(function(numRows) {
            var key = "csv/" + event.StreamName + "/" + sha1(event.Body) + ".csv";
            return csv.save(config.Bucket, key)
            .then(function() {
              event.S3Object = config.Bucket + "/" + key;
            });
          })
        });
      }
    })
    .then(function() {
      if (event.Metadata && event.Metadata.MultiThreading) {
        var handler = new csvMultiThreader.Handler(event, context, config);
        return handler.handle();
      } else {
        var handler = new csvProcessor.Handler(event, context, config);
        return handler.handle();
      }
    });
  });
}