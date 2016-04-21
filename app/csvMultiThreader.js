var Promise = require('bluebird'),
    AWS = require('aws-sdk'),
    csvHelper = require('../lib/csvHelper'),
    csv = new csvHelper.Csv(),
    conf = Promise.promisifyAll(require('aws-lambda-config')),
    s3 = Promise.promisifyAll(new AWS.S3()),
    s3SafeProcessor = require('../lib/s3SafeProcessor'),
    sha1 = require('sha1'),
    sns = Promise.promisifyAll(new AWS.SNS());

exports.Handler = function(event, context, config) {
  s3SafeProcessor.Handler.call(this, event, context, config);
}
exports.Handler.prototype = Object.create(s3SafeProcessor.Handler.prototype);

exports.Handler.prototype.getIterationMetadata = function() {
  return Promise.try(function() {
    return {
      Header: csv.header,
      MultiThreading: true
    }
  });
}

exports.Handler.prototype.load = function(objectBody) {
  var outer = this;
  var existingHeader = outer.event.Metadata ? outer.event.Metadata.Header : null;
  outer.currentBatch = 0;
  outer.batches = [];
  return csv.load(objectBody, existingHeader, outer.config.Delimiter)
  .then(function(bytesProcessed) {
    return csv.batchify({batchSize: outer.config.BatchSize || 1000})
    .then(function(batches) {
      outer.batches = batches;
      return bytesProcessed;
    });
  });
}

exports.Handler.prototype.processNext = function() {
  var outer = this;
  return Promise.try(function() {
    if (outer.currentBatch < outer.batches.length) {
      if (!outer.event.StreamName) throw new Error("Missing StreamName");
      var csvBatch = new csvHelper.Csv();
      csvBatch.body = outer.batches[outer.currentBatch];
      csvBatch.delimiter = outer.config.Delimiter;
      csvBatch.setHeader(csv.header);
      var key = "batch/" + outer.event.StreamName + "/" + sha1(csvBatch.body) + ".csv";
      return csvBatch.save(outer.config.Bucket, key)
      .then(function(csvString) {
        return outer.notify({
          S3Object: outer.config.Bucket + "/" + key,
          StreamName: outer.event.StreamName
        })
        .then(function(data) {
          outer.currentBatch++;
          return csvString.length;
        });
      });
    } else {
      return 0;
    }
  });
}
