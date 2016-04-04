var Promise = require('bluebird'),
    AWS = require('aws-sdk'),
    s3 = Promise.promisifyAll(new AWS.S3()),
    sns = Promise.promisifyAll(new AWS.SNS());

// config is optional
exports.Handler = function(event, context, config) {
  this.event = event;
  this.context = context;
  this.config = config || {};
}

exports.Handler.prototype.handle = function() {
  var outer = this;
  this.firstRun = true;
  return Promise.try(function() {
    var maxIterations = outer.config.MaxIterations || 100;
    if (outer.event.Iteration && outer.event.Iteration > maxIterations) throw new Error("StackOverflow: Iterated through SNS more than " + maxIterations + " times");
    if (outer.event.S3Object) {
      var delimiterLocation = outer.event.S3Object.indexOf('/');
      if (delimiterLocation > 0 && delimiterLocation != (outer.event.S3Object.length - 1)) {
        outer.bucket = outer.event.S3Object.substr(0, delimiterLocation);
        outer.key = outer.event.S3Object.substr(delimiterLocation + 1);
        return s3.headObjectAsync({
          Bucket: outer.bucket,
          Key: outer.key
        })
        .then(function(objectData) {
          outer.metadata = objectData.Metadata;
          outer.currentByte = parseInt(objectData.Metadata ? objectData.Metadata["current-byte"] : null) || 0;
          outer.contentLength = objectData.ContentLength;
          var numBytes = Math.min(outer.config.NumBytes || 1000000, outer.contentLength - outer.currentByte);
          if (outer.currentByte < objectData.ContentLength) {
            return s3.getObjectAsync({
              Bucket: outer.bucket,
              Key: outer.key,
              Range: "bytes=" + outer.currentByte + "-" + (outer.currentByte + numBytes - 1)
            })
            .then(function(objectData) {
              return objectData ? objectData.Body : "";
            });
          } else {
            console.warn({Message: outer.event.S3Object + " already completed processing."});
          }
        })
      } else {
        throw new Error("Invalid S3Object: " + outer.event.S3Object);
      }
    } else if (outer.event.Body) {
      outer.currentByte = 0;
      outer.contentLength = outer.event.Body.length;
      return outer.event.Body;
    } else {
      throw new Error("S3Object or Body is required in the event: " + JSON.stringify(outer.event));
    }
  })
  .then(function(objectBody) {
    var bytesProcessed = 0;
    if (objectBody) {
      return outer.load(objectBody)
      .then(function(loadBytesProcessed) {
        bytesProcessed += loadBytesProcessed;
        outer.currentByte += loadBytesProcessed;
        return outer.promiseWhile(
          function() {
            return bytesProcessed >= objectBody.length &&
              outer.currentByte >= outer.contentLength;
          },
          function() {
            return !outer.firstRun &&
              (
                outer.context.getRemainingTimeInMillis() < (outer.config.CutoffMillis || 1000)
                || bytesProcessed >= objectBody.length
              );
          },
          function() {
            return outer.processNext()
            .then(function(currentBytesProcessed) {
              outer.firstRun = false;
              outer.currentByte += currentBytesProcessed;
              bytesProcessed += currentBytesProcessed;
            });
          }
        )
      })
      .then(function(processingData) {
        processingData = processingData || {};
        processingData.BytesProcessed = bytesProcessed;
        return processingData;
      });
    }
  })
  .then(function(data) {
    outer.context.succeed(data);
  })
  .catch(function(err) {
    outer.error(err);
  });
}

exports.Handler.prototype.error = function(err) {
  console.error(err.message || err);
  this.context.fail(err);
  throw err;
}

// Should return a Promise. Implement this if you would like to pass any metadata with the SNS
// message that triggers the next processing iteration.
exports.Handler.prototype.getIterationMetadata = function() {
  return Promise.try(function() {});
}

// Should return a Promise
exports.Handler.prototype.load = function(objectBody) {
  throw new Error("Not implemented");
}

// Should return a Promise; must return the farthest byte that has been processed.
exports.Handler.prototype.processNext = function() {
  throw new Error("Not implemented");
}

// Gets the ARN of the SNS topic to publish to, which should kick off further
// processing by the same function.
exports.Handler.prototype.getNotificationArn = function() {
  return sns.createTopicAsync({
    Name: this.config.NotificationTopic || this.context.functionName
  })
  .then(function(topicData) {
    return topicData.TopicArn;
  });
}

// Notifies that the same function should recurse on the data.
exports.Handler.prototype.notify = function(event) {
  return this.getNotificationArn()
  .then(function(notificationArn) {
    return sns.publishAsync({
      Message: JSON.stringify(event),
      TopicArn: notificationArn
    })
  })
  .then(function(publishData) {
    return { NextIterationMessageId: publishData.MessageId };
  });
}

// Keeps track of how far into the S3 object that processing has been completed.
exports.Handler.prototype.updateCurrentByte = function() {
  this.metadata = this.metadata || {};
  this.metadata["current-byte"] = this.currentByte.toString();
  return s3.copyObjectAsync({
    Bucket: this.bucket,
    Key: this.key,
    CopySource: this.bucket + "/" + this.key,
    Metadata: this.metadata,
    MetadataDirective: 'REPLACE'
  });
}

exports.Handler.prototype.promiseWhile = Promise.method(function(processingComplete, iterate, action) {
  var outer = this;
  if (processingComplete()) return outer.updateCurrentByte();
  if (iterate()) {
    return outer.updateCurrentByte()
    .then(function() {
      return outer.getIterationMetadata();
    })
    .then(function(metadata) {
      var newEvent = outer.event;
      newEvent.Iteration = (newEvent.Iteration || 0) + 1;
      if (metadata) newEvent.Metadata = metadata;
      if (newEvent.Body) delete newEvent.Body;
      return outer.notify(newEvent);
    });
  }
  return action().then(outer.promiseWhile.bind(outer, processingComplete, iterate, action));
});