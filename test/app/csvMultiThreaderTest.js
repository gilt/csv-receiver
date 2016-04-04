var assert = require('assert'),
    sinon = require('sinon'),
    AWS = require('aws-sdk'),
    testHelpers = require('../helpers');

describe('csvMultiThreader', function() {
  var ctx = testHelpers.clearContext();
  var csvMultiThreader, conf;

  before(function() {
    sinon.stub(AWS, 'S3').returns(testHelpers.s3);
    sinon.stub(AWS, 'SNS').returns(testHelpers.sns);
    csvMultiThreader = testHelpers.require('../app/csvMultiThreader');
    conf = testHelpers.require('aws-lambda-config');
  });

  after(function() {
    AWS.S3.restore();
    AWS.SNS.restore();
  });

  describe('#load()', function() {
    it('should load the csv and separate it into batches', function() {
      var handler = new csvMultiThreader.Handler({}, ctx);
      return handler.load("foo,bar\n1,2\n3,4")
      .then(function(bytesProcessed) {
        assert.equal(bytesProcessed, 8);
        assert.equal(handler.batches.length, 1);
        assert.deepEqual(handler.batches[0], [['1', '2'], ['3', '4']]);
      });
    });

    it('should determine batch size based on optional config param BatchSize', function() {
      var handler = new csvMultiThreader.Handler({}, ctx, {BatchSize: 2});
      return handler.load("foo,bar\n1,2\n3,4\n5,6")
      .then(function(bytesProcessed) {
        assert.equal(bytesProcessed, 8);
        assert.equal(handler.batches.length, 2);
        assert.deepEqual(handler.batches[0], [['1', '2'], ['3', '4']]);
        assert.deepEqual(handler.batches[1], [['5', '6']]);
      });
    });

    it('should load the existing header from optional Metadata.Header', function() {
      var handler = new csvMultiThreader.Handler({Metadata: {Header: ["bal", "baz"]}}, ctx);
      return handler.load("foo,bar\n1,2\n3,4")
      .then(function(bytesProcessed) {
        assert.equal(bytesProcessed, 0);
        assert.equal(handler.batches.length, 1);
        assert.deepEqual(handler.batches[0], [['foo', 'bar'], ['1', '2'], ['3', '4']]);
      });
    });
  });

  describe('#processNext()', function() {
    it('should save all batches to S3 and notify to SNS', function() {
      testHelpers.s3.clear();
      testHelpers.sns.messages = [];
      var handler = new csvMultiThreader.Handler({StreamName: "test"}, ctx, {BatchSize: 2, Bucket: "bucket"});
      return handler.load("foo,bar\n1,2\n3,4\n5,6")
      .then(function() {
        return handler.processNext()
        .then(function(bytesProcessed) {
          assert.equal(bytesProcessed, 8);
          assert.equal(Object.keys(testHelpers.s3.objects).length, 1);
          assert.equal(testHelpers.s3.objects['bucket/batch/test/1489f923c4dca729178b3e3233458550d8dddf29.csv'], "foo,bar\n1,2\n3,4\n");
          assert.equal(testHelpers.sns.messages.length, 1);
          assert.equal(testHelpers.sns.messages[0], "{\"S3Object\":\"bucket/batch/test/1489f923c4dca729178b3e3233458550d8dddf29.csv\",\"StreamName\":\"test\"}");
        })
      })
      .then(function() {
        return handler.processNext()
        .then(function(bytesProcessed) {
          assert.equal(bytesProcessed, 4);
          assert.equal(Object.keys(testHelpers.s3.objects).length, 2);
          assert.equal(testHelpers.s3.objects['bucket/batch/test/5ba93c9db0cff93f52b521d7420e43f6eda2784f.csv'], "foo,bar\n5,6\n");
          assert.equal(testHelpers.sns.messages.length, 2);
          assert.equal(testHelpers.sns.messages[1], "{\"S3Object\":\"bucket/batch/test/5ba93c9db0cff93f52b521d7420e43f6eda2784f.csv\",\"StreamName\":\"test\"}");
        })
      })
      .then(function() {
        return handler.processNext()
        .then(function(bytesProcessed) {
          assert.equal(bytesProcessed, 0);
          assert.equal(Object.keys(testHelpers.s3.objects).length, 2);
          assert.equal(testHelpers.sns.messages.length, 2);
        })
      })
    });

    it('should fail if the StreamName is not in the event', function() {
      var handler = new csvMultiThreader.Handler({}, ctx);
      return handler.load("foo,bar\n1,2\n3,4")
      .then(function() {
        return testHelpers.assertFailure(handler.processNext(), /Missing StreamName/);
      });
    });
  });

  describe('#handle()', function() {
    it('should iterate through SNS until complete (twice)', function() {
      testHelpers.s3.putObject({Bucket: "bucket", Key: "object", Body: "foo,bar\n1,2\n3,4\n5,6\n"}, function() {});
      var event = {S3Object: "bucket/object", StreamName: "test"};
      var config = {BatchSize: 2, Bucket: "bucket"};
      ctx.getRemainingTimeInMillis = function() { return 999 };
      
      var handler = new csvMultiThreader.Handler(event, ctx, config);
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 16, NextIterationMessageId: 'sns-message-id'});
        assert.equal(testHelpers.s3.objects['bucket/batch/test/1489f923c4dca729178b3e3233458550d8dddf29.csv'], "foo,bar\n1,2\n3,4\n");
      })
      .then(function() {
        ctx = testHelpers.clearContext();
        ctx.getRemainingTimeInMillis = function() { return 1001 };
        return handler.getIterationMetadata()
        .then(function(metadata) {
          event.Metadata = metadata;
          handler = new csvMultiThreader.Handler(event, ctx, config);
          return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
            assert.deepEqual(data, {BytesProcessed: 4, ETag: 's3-object-tag'});
            assert.equal(testHelpers.s3.objects['bucket/batch/test/5ba93c9db0cff93f52b521d7420e43f6eda2784f.csv'], "foo,bar\n5,6\n");
          });
        });
      });
    });
  });
});