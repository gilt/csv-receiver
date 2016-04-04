var assert = require('assert'),
    AWS = require('aws-sdk'),
    Promise = require('bluebird'),
    sinon = require('sinon'),
    testHelpers = require('../helpers');
var safeProcessor;

describe('s3SafeProcessor', function() {
  var s3 = {
    'content': 'foo',
    'CurrentByte': null,
    'getObject': function(params, callback) {
      var body = s3.content;
      if (params.Range) {
        var matches = params.Range.match(/bytes=(\d+)-(\d+)/);
        body = body.substring(parseInt(matches[1]), parseInt(matches[2]) + 1);
      }
      callback(null, {
        Body: body,
        Metadata: {"current-byte": s3.CurrentByte},
        ContentLength: s3.content.length
      });
    },
    'headObject': function(params, callback) {
      callback(null, {
        Metadata: {"current-byte": s3.CurrentByte},
        ContentLength: s3.content.length
      });
    },
    'copyObject': function(params, callback) {
      if (!params.CopySource) throw {"code": "MissingRequiredParameter", message: "Missing required key 'CopySource' in params"};
      if (params.Metadata && params.Metadata["current-byte"]) this.CurrentByte = params.Metadata["current-byte"];
      callback(null, {ETag: "s3-object-tag"});
    }
  };
  var sns = {'publish': function(){}, 'createTopic': function(params, callback) { callback(null, { TopicArn: "arn:aws:sns:us-east-1:1234567890:" + params.Name })}};
  var ctx, TestHandler;

  before(function() {
    sinon.stub(AWS, 'S3').returns(s3);
    sinon.stub(AWS, 'SNS').returns(sns);
    safeProcessor = testHelpers.require('../lib/s3SafeProcessor');

    TestHandler = function(event, context, config) {
      this.processBlockSize = 1;
      this.currentBlock = 0;
      safeProcessor.Handler.call(this, event, context, config);
    }
    TestHandler.prototype = Object.create(safeProcessor.Handler.prototype);
    TestHandler.prototype.load = function(objectBody) {
      var outer = this;
      return Promise.try(function() {
        outer.processed = [];
        outer.objectBody = objectBody;
        return 0;
      });
    }
    TestHandler.prototype.processNext = function() {
      var outer = this;
      return Promise.try(function() {
        var processBlock = outer.objectBody.substr(outer.currentBlock, outer.processBlockSize);
        outer.currentBlock += outer.processBlockSize;
        outer.processed.push(processBlock);
        return processBlock.length;
      });
    }
    TestHandler.prototype.assertProcessed = function(processedString, numBlocks) {
      assert.equal(processedString, this.processed.join(''));
      assert.equal(this.processed.length, numBlocks);
    }
  });

  after(function() {
    AWS.S3.restore();
    AWS.SNS.restore();
  });

  describe('#getNotificationArn()', function() {
    before(function() {
      ctx = testHelpers.clearContext();
    });

    it('should return the topic ARN based on the StreamName', function() {
      var handler = new TestHandler({}, ctx);
      return handler.getNotificationArn()
      .then(function(arn) {
        assert.equal(arn, "arn:aws:sns:us-east-1:1234567890:test");
      });
    });

    it('should return the topic ARN based on the config', function() {
      var handler = new TestHandler({}, ctx, { NotificationTopic: "foo-bar" });
      return handler.getNotificationArn()
      .then(function(arn) {
        assert.equal(arn, "arn:aws:sns:us-east-1:1234567890:foo-bar");
      });
    });
  });

  describe('#handle()', function() {
    beforeEach(function() {
      s3.CurrentByte = null;
      s3.content = 'foo';
      ctx = testHelpers.clearContext();
      sinon.stub(sns, 'createTopic').yields(null, {TopicArn: "arn:aws:sns:us-east-1:1234567890:test-topic"});
      sinon.stub(sns, 'publish').yields(null, {MessageId: "sns-message-id"});
    });

    afterEach(function() {
      sns.createTopic.restore();
      sns.publish.restore();
    });

    it('should fail if the S3Object and Body are missing from the event', function() {
      var handler = new TestHandler({}, ctx);
      return testHelpers.assertFailure(handler.handle(), "S3Object or Body is required");
    });

    it('should fail if the S3Object is missing the bucket', function() {
      var handler = new TestHandler({S3Object: "/object"}, ctx);
      return testHelpers.assertFailure(handler.handle(), "Invalid S3Object");
    });

    it('should fail if the S3Object is missing the object', function() {
      var handler = new TestHandler({S3Object: "bucket/"}, ctx);
      return testHelpers.assertFailure(handler.handle(), "Invalid S3Object");
    });

    it('should fail if the S3Object is missing the bucket/object structure', function() {
      var handler = new TestHandler({S3Object: "bucket"}, ctx);
      return testHelpers.assertFailure(handler.handle(), "Invalid S3Object");
    });

    it('should do no processing if the CurrentByte is at the end of the stream', function() {
      s3.CurrentByte = s3.content.length;
      var handler = new TestHandler({S3Object: "bucket/object"}, ctx);
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) { assert.equal(data, null)});
    });

    it('should process from the Body directly', function() {
      var handler = new TestHandler({Body: "foo"}, ctx);
      handler.processBlockSize = 3;
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 3, ETag: 's3-object-tag'});
        handler.assertProcessed('foo', 1);
      });
    });

    it('should process one entire block', function() {
      var handler = new TestHandler({S3Object: "bucket/object"}, ctx);
      handler.processBlockSize = s3.content.length;
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 3, ETag: 's3-object-tag'});
        handler.assertProcessed('foo', 1);
      });
    });

    it('should process multiple blocks in one iteration', function() {
      ctx.getRemainingTimeInMillis = function() { return 1001 };
      var handler = new TestHandler({S3Object: "bucket/object"}, ctx);
      handler.processBlockSize = 1;
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 3, ETag: 's3-object-tag'});
        handler.assertProcessed('foo', 3);
      });
    });

    it('should run at least once when processing', function() {
      ctx.getRemainingTimeInMillis = function() { return 999 };
      var handler = new TestHandler({S3Object: "bucket/object"}, ctx);
      handler.processBlockSize = 1;
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 1, NextIterationMessageId: 'sns-message-id'});
        handler.assertProcessed('f', 1);
      });
    });

    it('should iterate through SNS until complete (twice)', function() {
      s3.content = 'bar'
      ctx.getRemainingTimeInMillis = function() { return 999 };
      var handler = new TestHandler({S3Object: "bucket/object"}, ctx);
      handler.processBlockSize = 1;
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 1, NextIterationMessageId: 'sns-message-id'});
        handler.assertProcessed('b', 1);
      })
      .then(function() {
        ctx = testHelpers.clearContext();
        ctx.getRemainingTimeInMillis = function() { return 1001 };
        handler = new TestHandler({S3Object: "bucket/object"}, ctx);
        return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
          assert.deepEqual(data, {BytesProcessed: 2, ETag: 's3-object-tag'});
          handler.assertProcessed('ar', 2);
        });
      });
    });

    it('should listen to CutoffMillis in config', function() {
      s3.content = 'bar'
      ctx.getRemainingTimeInMillis = function() { return 1001 };
      var handler = new TestHandler({S3Object: "bucket/object"}, ctx, {CutoffMillis: 1002});
      handler.processBlockSize = 1;
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 1, NextIterationMessageId: 'sns-message-id'});
        handler.assertProcessed('b', 1);
      })
    });

    it('should iterate through SNS until complete (thrice)', function() {
      s3.content = 'bar'
      ctx.getRemainingTimeInMillis = function() { return 999 };
      var handler = new TestHandler({S3Object: "bucket/object"}, ctx);
      handler.processBlockSize = 1;
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 1, NextIterationMessageId: 'sns-message-id'});
        handler.assertProcessed('b', 1);
      })
      .then(function() {
        ctx = testHelpers.clearContext();
        ctx.getRemainingTimeInMillis = function() { return 999 };
        handler = new TestHandler({S3Object: "bucket/object"}, ctx);
        return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
          assert.deepEqual(data, {BytesProcessed: 1, NextIterationMessageId: 'sns-message-id'});
          handler.assertProcessed('a', 1);
        });
      })
      .then(function() {
        ctx = testHelpers.clearContext();
        ctx.getRemainingTimeInMillis = function() { return 999 };
        handler = new TestHandler({S3Object: "bucket/object"}, ctx);
        return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
          assert.deepEqual(data, {BytesProcessed: 1, ETag: 's3-object-tag'});
          handler.assertProcessed('r', 1);
        });
      });
    });

    it('should listen to NumBytes in config', function() {
      ctx.getRemainingTimeInMillis = function() { return 1001 };
      var handler = new TestHandler({S3Object: "bucket/object"}, ctx, {NumBytes: 2});
      handler.processBlockSize = 100;
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 2, NextIterationMessageId: 'sns-message-id'});
        handler.assertProcessed('fo', 1);
      });
    });

    it('should update the number of iterations', function() {
      sns.publish.restore();
      var snsStub = sinon.stub(sns, 'publish');
      snsStub.withArgs({
        Message: "{\"S3Object\":\"bucket/object\",\"Iteration\":1}",
        TopicArn: "arn:aws:sns:us-east-1:1234567890:test-topic"
      }).yields(null, {MessageId: "sns-message-id"});
      snsStub.yields({"message": "Invalid args"}, null);
      
      var handler = new TestHandler({S3Object: "bucket/object"}, ctx, {NumBytes: 2});
      handler.processBlockSize = 100;
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 2, NextIterationMessageId: 'sns-message-id'});
        handler.assertProcessed('fo', 1);
      });
    });

    it('should stop attempting to process if it iterates too many times', function() {
      var handler = new TestHandler({S3Object: "bucket/object", Iteration: 101}, ctx, {NumBytes: 2});
      return testHelpers.assertContextFailure(handler.handle(), ctx, /StackOverflow/);
    });

    it('should stop attempting to process if it iterates too many times, with MaxIterations in config', function() {
      var handler = new TestHandler({S3Object: "bucket/object", Iteration: 11}, ctx, {NumBytes: 2, MaxIterations: 10});
      return testHelpers.assertContextFailure(handler.handle(), ctx, /StackOverflow/);
    });

    it('should pass metadata to the SNS message when given', function() {
      var handler = new TestHandler({S3Object: "bucket/object", Iteration: 1}, ctx, {NumBytes: 2});
      handler.processBlockSize = 100;
      handler.getIterationMetadata = function() {
        return Promise.try(function() {
          return { Header: "header"}
        });
      }
      sns.publish.restore();
      var snsStub = sinon.stub(sns, 'publish');
      snsStub.withArgs({
        Message: "{\"S3Object\":\"bucket/object\",\"Iteration\":2,\"Metadata\":{\"Header\":\"header\"}}",
        TopicArn: "arn:aws:sns:us-east-1:1234567890:test-topic"
      }).yields(null, {MessageId: "sns-message-id"});
      snsStub.yields({"message": "Invalid args"}, null);
      
      return testHelpers.assertContextSuccess(handler.handle(), ctx, function(data) {
        assert.deepEqual(data, {BytesProcessed: 2, NextIterationMessageId: 'sns-message-id'});
        handler.assertProcessed('fo', 1);
      });
    });
  });

  describe('#notify()', function() {
    beforeEach(function() {
      ctx = testHelpers.clearContext();
    });

    afterEach(function() {
      sns.publish.restore();
    });

    it('should notify the topic ARN based on the StreamName', function() {
      var handler = new TestHandler({}, ctx);
      var snsStub = sinon.stub(sns, 'publish');
      snsStub.withArgs({
        Message: "{\"foo\":\"bar\"}",
        TopicArn: "arn:aws:sns:us-east-1:1234567890:test"
      }).yields(null, {MessageId: "sns-message-id"});
      snsStub.yields({"message": "Invalid args"}, null);
      return handler.notify({foo: "bar"})
      .then(function(arn) {
        assert.deepEqual(arn, { NextIterationMessageId: "sns-message-id" });
      });
    });

    it('should notify the topic ARN based on the config', function() {
      var handler = new TestHandler({}, ctx, { NotificationTopic: "foo-bar" });
      var snsStub = sinon.stub(sns, 'publish');
      snsStub.withArgs({
        Message: "{\"foo\":\"bar\"}",
        TopicArn: "arn:aws:sns:us-east-1:1234567890:foo-bar"
      }).yields(null, {MessageId: "sns-message-id"});
      snsStub.yields({"message": "Invalid args"}, null);
      return handler.notify({foo: "bar"})
      .then(function(arn) {
        assert.deepEqual(arn, { NextIterationMessageId: "sns-message-id" });
      });
    });
  });
});
