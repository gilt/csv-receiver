var Promise = require('bluebird'),
    AWS = require('aws-sdk'),
    csv = Promise.promisifyAll(require('csv')),
    snsRaw = new AWS.SNS(),
    sns = Promise.promisifyAll(snsRaw);

// Breaks up wht CSV into JSON messages, which are then sent into the sendMessage function.
// sendMessage should return a Promise.
exports.processCsv = function(csvBody, sendMessage) {
  return csv
  .parseAsync(csvBody || '')
  .then(function(csvDoc) {
    var header = csvDoc.shift();
    return Promise.map(
      csvDoc,
      function(row) {
        var message = {};
        for (var i = 0; i < header.length; i++) {
          exports.addToObject(message, exports.compactPath(header[i]), row[i]);
        }
        return sendMessage(message);
      }
    );
  });
}

// Assumes 'name' is a compactPath (see below), to avoid repeatedly calling
// compactPath as it recurses.
exports.addToObject = function(obj, name, value) {
  var path = name.split(".");
  var currentProperty = path.shift();
  if (path.length > 0) {
    obj[currentProperty] = typeof obj[currentProperty] == "object" && obj[currentProperty] != null ? obj[currentProperty] : {};
    exports.addToObject(obj[currentProperty], path.join("."), value);
  } else {
    obj[currentProperty] = value;
  }
}

// Removes extra spaces and periods, so the string can be interpereted
// as a JSON path.
exports.compactPath = function(str) {
  return str
  .split(".")
  .map(function(s){return s.trim();})
  .filter(function(e){return e.replace(" ", "")})
  .join(".");
}