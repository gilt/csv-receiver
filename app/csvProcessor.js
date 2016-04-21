var Promise = require('bluebird'),
    AWS = require('aws-sdk'),
    csvHelper = require('../lib/csvHelper'),
    csv = new csvHelper.Csv(),
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
      Processing: true
    }
  });
}

exports.Handler.prototype.load = function(objectBody) {
  var outer = this;
  return Promise.try(function() {
    if (!outer.event.StreamName) throw new Error("Missing StreamName");
    var existingHeader = outer.event.Metadata ? outer.event.Metadata.Header : null;
    return csv.load(objectBody, existingHeader, outer.config.Delimiter)
    .then(function(data) {
      if (!outer.event.StreamArn) {
        return sns
        .createTopicAsync({Name: outer.event.StreamName})
        .then(function(topicData) {
          outer.event.StreamArn = topicData.TopicArn;
          return data;
        });
      } else {
        return data;
      }
    });
  });
}

exports.Handler.prototype.processNext = function() {
  return csv.processNext(this.event.StreamArn);
}
