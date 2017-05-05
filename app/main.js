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
      if (event.Records) {
        if (event.Records.length > 1) console.error("Event has more than one Record and the last " + event.Records.length + " Records were not processed: " + JSON.stringify(event));
        if (event.Records[0].Sns) return JSON.parse(event.Records[0].Sns.Message);
        if (event.Records[0].s3) {
          var bucket = event.Records[0].s3.bucket.name;
          var objectKey = event.Records[0].s3.object.key;
          var cleanObjectKey = objectKey.replace(/^raw\//, '');
          var streamName = cleanObjectKey.substr(0, cleanObjectKey.lastIndexOf('/'));
          console.info('Processing ' + bucket + '/' + objectKey);
          return s3.getObjectAsync({
            Bucket: bucket,
            Key: objectKey
          })
          .then(function(objectData) {
            return {
              Body: objectData ? objectData.Body.toString() : "",
              S3Object: bucket + "/" + objectKey,
              StreamName: streamName,
              Metadata: {
                MultiThreading: true,
                RawLocation: bucket + "/" + objectKey
              }
            };
          });
        }
      }
      return event;
    })
    .then(function(event) {
      if (event.Body && !(event.Metadata && event.Metadata.RawLocation)) {
        // Process the Body of the CSV
        if (!config.Bucket) throw new Error("Missing S3 bucket for stream");
        if (!event.StreamName) throw new Error("Missing StreamName for processing csv body");
        var objectKey = "raw/" + event.StreamName + "/" + sha1(event.Body) + ".txt";
        // Save the raw file
        return s3.putObjectAsync({
          Bucket: config.Bucket,
          Key: objectKey,
          Body: event.Body
        })
        .then(function() {
          return concat(event, {Metadata: {RawLocation: config.Bucket + "/" + objectKey}});
        });
      } else {
        return event;
      }
    })
    .then(function(event) {
      if (event.Body) {
        var csv = new csvHelper.Csv();
        return csv.load(event.Body, null, config.Delimiter)
        .then(function(numChars) {
          console.info('Rows in CSV file: ' + csv.body.length);
          var key = "csv/" + event.StreamName + "/" + sha1(event.Body) + ".csv";
          return csv.save(config.Bucket, key)
          .then(function() {
            return concat(event, {Metadata: {MultiThreading: true}, S3Object: config.Bucket + "/" + key});
          });
        })
      } else {
        return event;
      }
    })
    .then(function(event) {
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

function concat(obj1, obj2) {
  var result = JSON.clone(obj1);
  Object.keys(obj2).forEach(function(key) {
    if (isObject(result[key]))
      result[key] = concat(result[key], obj2[key]);
    else
      result[key] = obj2[key];
  });
  return result;
}

function isObject(obj) {
  return (typeof obj === "object") && (obj !== null)
}

if (typeof JSON.clone !== "function")
{
    JSON.clone = function(obj)
    {
        return JSON.parse(JSON.stringify(obj));
    };
}