var Promise = require('bluebird'),
    AWS = require('aws-sdk'),
    csv = Promise.promisifyAll(require('csv')),
    s3 = Promise.promisifyAll(new AWS.S3()),
    sns = Promise.promisifyAll(new AWS.SNS());

exports.Csv = function() {
  this.rawBody = '';
  this.body = [];
  this.delimiter = null;
  this.header = null;
  this.headerString = '';
  this.currentRow = 0;
}

// Returns the number of bytes "processed"
exports.Csv.prototype.load = function(rawBody, existingHeader, delimiter) {
  var outer = this;
  outer.rawBody = rawBody || '';
  outer.delimiter = delimiter;
  return csv
  .parseAsync(outer.rawBody, {delimiter: outer.delimiter})
  .then(function(csvDoc) {
    var newHeaderProcessed = false;
    var header = existingHeader;
    // Assume no header if it comes in through the optional parameter.
    if (!header) {
      header = csvDoc.shift();
      newHeaderProcessed = true;
    }
    outer.body = csvDoc;
    outer.currentRow = 0;
    return outer.setHeader(header)
    .then(function(numBytes) {
      if (newHeaderProcessed) return numBytes
      else return 0;
    });
  });
}

// Breaks the CSV up into smaller batches of batchSize or numBatches batches. Must specify one or
// the other. If both are specified, batchSize will be used. Can also pass an optional processedOnly
// parameter, which will skip processed rows when creating the batches.
// Batches do not include the header and are returned as arrays [csv rows] of arrays [csv values].
exports.Csv.prototype.batchify = function(opts) {
  var outer = this;
  return Promise.try(function() {
    opts = opts || {};
    var unprocessedOnly = opts.unprocessedOnly || false;
    var startIndex = unprocessedOnly ? outer.currentRow : 0;
    var batchSize = opts.batchSize;
    var numBatches = opts.numBatches;
    if (batchSize) {
      numBatches = Math.ceil((outer.body.length - startIndex) / batchSize);
    } else if (numBatches) {
      batchSize = Math.ceil((outer.body.length - startIndex) / numBatches);
      numBatches = Math.ceil((outer.body.length - startIndex) / batchSize);
    } else {
      throw new Error("Either batchSize or numBatches is required.")
    }
    return Promise.map(
      Array.apply(null, Array(numBatches)).map(function(x,i) {return i}),
      function(i) {
        return outer.body.slice(i * batchSize + startIndex, (i * batchSize) + batchSize + startIndex);
      }
    );
  });
}

// Breaks up the CSV into JSON messages, which are then sent into the sendMessage function.
// sendMessage should return a Promise.
exports.Csv.prototype.processNext = function(topicArn) {
  var outer = this;
  return Promise.try(function() {
    if (!topicArn) throw new Error("Missing required topicArn for publishing CSV rows to the topic");
    var row = outer.body[outer.currentRow];
    if (row) {
      var message = {};
      for (var i = 0; i < outer.header.length; i++) {
        outer.addToObject(message, outer.compactPath(outer.header[i]), row[i]);
      }
      return sns.publishAsync({
        Message: JSON.stringify(message),
        TopicArn: topicArn
      })
      .then(function(messageData) {
        console.log("Published message to SNS: " + messageData.MessageId);
        return csv.stringifyAsync([row], {delimiter: outer.delimiter});
      })
      .then(function(rowBody) {
        outer.currentRow++;
        return rowBody.length;
      });
    } else {
      return 0;
    }
  });
}

// Assumes 'name' is a compactPath (see below), to avoid repeatedly calling
// compactPath as it recurses.
exports.Csv.prototype.addToObject = function(obj, name, value) {
  var path = name.split(".");
  var currentProperty = path.shift();
  if (path.length > 0) {
    obj[currentProperty] = typeof obj[currentProperty] == "object" && obj[currentProperty] != null ? obj[currentProperty] : {};
    this.addToObject(obj[currentProperty], path.join("."), value);
  } else {
    obj[currentProperty] = value;
  }
}

// Removes extra spaces and periods, so the string can be interpereted
// as a JSON path.
exports.Csv.prototype.compactPath = function(str) {
  return str
  .split(".")
  .map(function(s){return s.trim();})
  .filter(function(e){return e.replace(" ", "")})
  .join(".");
}

// Saves the internal CSV to S3, essentially converting a raw file to a common CSV format.
exports.Csv.prototype.save = function(bucket, key) {
  var outer = this;
  return Promise.try(function() {
    if (!bucket) throw new Error("Missing S3 bucket");
    if (!key) throw new Error("Missing object key");
    return csv.stringifyAsync(outer.body, {delimiter: outer.delimiter})
    .then(function(csvBody) {
      return s3.putObjectAsync({
        Bucket: bucket,
        Key: key,
        Body: outer.headerString + csvBody
      })
      .then(function(data) {
        return csvBody;
      });
    });
  });
}

// Sets the header to the given one (must be an array of header values)
exports.Csv.prototype.setHeader = function(header) {
  var outer = this;
  outer.header = header;
  return csv.stringifyAsync([outer.header], {delimiter: outer.delimiter})
  .then(function(headerString) {
    outer.headerString = headerString;
    return outer.headerString.length
  });
}