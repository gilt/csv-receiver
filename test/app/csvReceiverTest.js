var assert = require('assert'),
    sinon = require('sinon'),
    AWS = require('aws-sdk'),
    context = require('aws-lambda-mock-context');

describe('csvReceiver', function() {
  var sns = {'publish': function(){}, 'createTopic': function(){}};
  var csvReceiver;

  describe('#httpPost()', function() {
    before(function () {
      sinon
        .stub(sns, 'publish')
        .yields(null, {"MessageId": "12345"});
      sinon
        .stub(sns, 'createTopic')
        .yields(null, {"TopicArn": "arn:aws:sns:us-east-1:1234567890:StreamName"});
      sinon.stub(AWS, 'SNS').returns(sns);
      csvReceiver = require('../../app/csvReceiver.js');
    });

    after(function() {
      AWS.SNS.restore();
    });

    it('should work with happy path', function() {
      const ctx = context();

      return assertSuccess(
        csvReceiver.httpPost({Body: 'foo,bar\n1,2\n3,4', StreamName: "test-stream"}, ctx),
        ctx,
        function(results) {
          assert.equal(results.length, 2);
        }
      );
    });

    it('should fail if StreamName is missing', function() {
      const ctx = context();

      return assertFailure(
        csvReceiver.httpPost({Body: 'foo,bar\n1,2\n3,4'}, ctx),
        ctx,
        "Missing StreamName"
      );
    });
  });
});

function assertSuccess(promise, context, check) {
  return promise
  .then(function() {
    return context.Promise
    .then(function(results) {
      return check(results);
    })
  })
  .catch(assert.ifError);
}

function assertFailure(promise, context, message) {
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