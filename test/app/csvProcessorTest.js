var assert = require('assert'),
    sinon = require('sinon'),
    AWS = require('aws-sdk'),
    testHelpers = require('../helpers');

describe('csvMultiThreader', function() {
  var ctx = testHelpers.clearContext();
  var csvProcessor, conf;

  before(function() {
    sinon.stub(AWS, 'S3').returns(testHelpers.s3);
    sinon.stub(AWS, 'SNS').returns(testHelpers.sns);
    csvProcessor = testHelpers.require('../app/csvProcessor');
    conf = testHelpers.require('aws-lambda-config');
  });

  after(function() {
    AWS.S3.restore();
    AWS.SNS.restore();
  });

  describe('#load()', function() {
    it('should load the csv and generate the StreamArn', function() {
      var handler = new csvProcessor.Handler({StreamName: "test"}, ctx);
      return handler.load("foo,bar\n1,2\n3,4")
      .then(function(bytesProcessed) {
        assert.equal(bytesProcessed, 8);
        assert.equal(handler.event.StreamArn, "arn:aws:sns:us-east-1:1234567890:test");
      });
    });

    it('should not produce the StreamArn if already present', function() {
      sinon.stub(testHelpers.sns, 'publish').yields({"message": "Invalid args"}, null);
      var handler = new csvProcessor.Handler({StreamName: "test", StreamArn: "arn:aws:sns:us-east-1:1234567890:test2"}, ctx, {BatchSize: 2});
      return handler.load("foo,bar\n1,2\n3,4\n5,6")
      .then(function(bytesProcessed) {
        assert.equal(bytesProcessed, 8);
        assert.equal(handler.event.StreamArn, "arn:aws:sns:us-east-1:1234567890:test2");
        testHelpers.sns.publish.restore();
      });
    });

    it('should load the existing header from optional Metadata.Header', function() {
      var handler = new csvProcessor.Handler({StreamName: "test", Metadata: {Header: ["bal", "baz"]}}, ctx);
      return handler.load("foo,bar\n1,2\n3,4")
      .then(function(bytesProcessed) {
        assert.equal(bytesProcessed, 0);
        assert.equal(handler.event.StreamArn, "arn:aws:sns:us-east-1:1234567890:test");
      });
    });

    it('should fail if the StreamName is missing', function() {
      var handler = new csvProcessor.Handler({}, ctx);
      return testHelpers.assertFailure(handler.load("foo,bar\n1,2\n3,4\n5,6"), /Missing StreamName/);
    });
  });

  describe('#processNext()', function() {
    it('should publish messages to SNS', function() {
      testHelpers.s3.clear();
      testHelpers.sns.messages = [];
      var handler = new csvProcessor.Handler({StreamName: "test"}, ctx);
      return handler.load("foo,bar\n1,2\n3,4\n5,6")
      .then(function() {
        return handler.processNext()
        .then(function(bytesProcessed) {
          assert.equal(bytesProcessed, 4);
          assert.equal(testHelpers.sns.messages.length, 1);
          assert.equal(testHelpers.sns.messages[0], "{\"foo\":\"1\",\"bar\":\"2\"}");
        })
      })
      .then(function() {
        return handler.processNext()
        .then(function(bytesProcessed) {
          assert.equal(bytesProcessed, 4);
          assert.equal(testHelpers.sns.messages.length, 2);
          assert.equal(testHelpers.sns.messages[1], "{\"foo\":\"3\",\"bar\":\"4\"}");
        })
      })
      .then(function() {
        return handler.processNext()
        .then(function(bytesProcessed) {
          assert.equal(bytesProcessed, 4);
          assert.equal(testHelpers.sns.messages.length, 3);
          assert.equal(testHelpers.sns.messages[2], "{\"foo\":\"5\",\"bar\":\"6\"}");
        })
      })
      .then(function() {
        return handler.processNext()
        .then(function(bytesProcessed) {
          assert.equal(bytesProcessed, 0);
          assert.equal(testHelpers.sns.messages.length, 3);
        })
      })
    });
  });

  describe('#handle()', function() {
    it('should iterate through SNS until complete (twice)', function() {
      testHelpers.sns.clear();
      testHelpers.s3.putObject({Bucket: "bucket", Key: "object", Body: "foo,bar\n1,2\n3,4\n5,6\n"}, function() {});
      var event = {S3Object: "bucket/object", StreamName: "test"};
      var config = {};
      ctx.getRemainingTimeInMillis = function() { return 999 };
      
      var handler = new csvProcessor.Handler(event, ctx, config);
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 12, NextIterationMessageId: 'sns-message-id'});
        assert.equal(testHelpers.sns.messages.length, 2);
        assert.equal(testHelpers.sns.messages[0], "{\"foo\":\"1\",\"bar\":\"2\"}");
        assert.equal(testHelpers.sns.messages[1], "{\"S3Object\":\"bucket/object\",\"StreamName\":\"test\",\"StreamArn\":\"arn:aws:sns:us-east-1:1234567890:test\",\"Iteration\":1,\"Metadata\":{\"Header\":[\"foo\",\"bar\"],\"Processing\":true}}");
      })
      .then(function() {
        ctx = testHelpers.clearContext();
        ctx.getRemainingTimeInMillis = function() { return 1001 };
        return handler.getIterationMetadata()
        .then(function(metadata) {
          event.Metadata = metadata;
          handler = new csvProcessor.Handler(event, ctx, config);
          return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
            assert.deepEqual(data, {BytesProcessed: 8, ETag: 's3-object-tag'});
            assert.equal(testHelpers.sns.messages.length, 4);
            assert.equal(testHelpers.sns.messages[2], "{\"foo\":\"3\",\"bar\":\"4\"}");
            assert.equal(testHelpers.sns.messages[3], "{\"foo\":\"5\",\"bar\":\"6\"}");
          });
        });
      });
    });
  });
});