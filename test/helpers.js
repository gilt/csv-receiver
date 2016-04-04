var assert = require('assert'),
    context = require('aws-lambda-mock-context');

exports.assertSuccess = function(promise, check) {
  return promise
  .then(function(results) {
    return check(results);
  })
  .catch(assert.ifError);
}

exports.assertContextSuccess = function(promise, context, check) {
  return promise
  .then(function() {
    return context.Promise
    .then(function(results) {
      return check(results);
    })
  })
  .catch(assert.ifError);
}

exports.assertFailure = function(promise, message) {
  return promise
  .then(function(results) {
    assert.ok(false, "Expected to throw an error, but was successful");
  })
  .catch(function(err) {
    if (err.name == "AssertionError")
      throw err;
    else
      assert((err.message || "").match(message), "Expected [" + err + "] to match [" + message + "] but it did not");
  });
}

exports.assertContextFailure = function(promise, context, message) {
  return promise
  .then(function() {
    return context.Promise
    .then(function(results) {
      assert.ok(false, "Expected to throw an error, but was successful");
    })
  })
  .catch(function(err) {
    if (err.name == "AssertionError")
      throw err;
    else
      assert(err.message.match(message), "Expected [" + err + "] to match [" + message + "] but it did not");
  });
}

exports.clearContext = function() {
  return context({
    region: "us-east-1",
    account: "1234567890",
    functionName: "test"
  });
}

exports.require = function(lib) {
  delete require.cache[require.resolve(lib)];
  return require(lib);
}

exports.s3 = {
  'objects': {},
  'metadata': {},
  'clear': function() {
    this.objects = {};
    this.metadata = {};
  },
  'copyObject': function(params, callback) {
    if (!params.CopySource) throw {"code": "MissingRequiredParameter", message: "Missing required key 'CopySource' in params"};
    var fullKey = params.Bucket + "/" + params.Key;
    if (params.Metadata) this.metadata[fullKey] = params.Metadata;
    callback(null, {"ETag": "s3-object-tag"});
  },
  'getObject': function(params, callback) {
    var fullKey = params.Bucket + "/" + params.Key;
    var object = this.objects[fullKey];
    if (params.Range) {
      var matches = params.Range.match(/bytes=(\d+)-(\d+)/);
      if (matches[1] && matches[2]) {
        object = object.substr(parseInt(matches[1]), parseInt(matches[2]));
      }
    }
    
    if (object) {
      callback(null, {Body: new Buffer(object), ContentLength: object.length, Metadata: this.metadata[fullKey] });
    } else {
      callback(new Error("Object [" + fullKey + "] not found"));
    }
  },
  'headObject': function(params, callback) {
    var fullKey = params.Bucket + "/" + params.Key;
    var object = this.objects[fullKey];
    
    if (object) {
      callback(null, { ContentLength: object.length, Metadata: this.metadata[fullKey] });
    } else {
      callback(new Error("Object [" + fullKey + "] not found"));
    }
  },
  'putObject': function(params, callback) {
    var fullKey = params.Bucket + "/" + params.Key;
    this.objects[fullKey] = params.Body;
    if (params.Metadata) this.metadata[fullKey] = params.Metadata;
    callback(null, {"ETag": "s3-object-tag"});
  }
}
exports.sns = {
  'messages': [],
  'clear': function() {
    this.messages = [];
  },
  'createTopic': function(params, callback) {
    callback(null, {
      TopicArn: "arn:aws:sns:us-east-1:1234567890:" + params.Name
    });
  },
  'publish': function(params, callback) {
    this.messages.push(params.Message);
    callback(null, {
      MessageId: "sns-message-id"
    });
  }
}
