var Promise = require('bluebird'),
    AWS = require('aws-sdk'),
    csvProcessor = require('../lib/csvProcessor'),
    sns = Promise.promisifyAll(new AWS.SNS());

// Receives a CSV via HTTP POST through API Gateway
exports.httpPost = function(event, context) {
  return Promise.try(function() {
    if (!event.StreamName) throw new Error("Missing StreamName");
    return sns
    .createTopicAsync({Name: event.StreamName})
    .then(function(data) {
      return csvProcessor.processCsv(event.Body, function(message) {
        return sns.publishAsync(message, data.TopicArn);
      })
      .then(function(messageData) {
        context.succeed(messageData);
      });
    })
  });
}