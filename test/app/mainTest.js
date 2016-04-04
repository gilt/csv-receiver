var assert = require('assert'),
    sinon = require('sinon'),
    AWS = require('aws-sdk'),
    testHelpers = require('../helpers');

describe('main', function() {
  var ctx = testHelpers.clearContext();
  var main;

  before(function() {
    sinon.stub(AWS, 'S3').returns(testHelpers.s3);
    sinon.stub(AWS, 'SNS').returns(testHelpers.sns);
    main = testHelpers.require('../app/main');
  });

  after(function() {
    AWS.S3.restore();
    AWS.SNS.restore();
  });

  describe('#handle()', function() {
    beforeEach(function() {
      testHelpers.s3.clear();
      testHelpers.sns.clear();
      ctx = testHelpers.clearContext();
    });

    it('should save the raw, sanitized csv, and batched csv files to S3', function() {
      testHelpers.s3.putObject({
        Bucket: "aws.lambda.us-east-1.1234567890.config",
        Key: "test.json",
        Body: "{\"Bucket\":\"bucket\"}"
      }, function() {});
      return testHelpers.assertContextSuccess(
        main.handle({Body: 'foo,bar\n1,2\n3,4', StreamName: "test"}, ctx),
        ctx,
        function(results) {
          assert.deepEqual(results, { ETag: 's3-object-tag', BytesProcessed: 16 });
          assert.equal(testHelpers.s3.objects["bucket/raw/test/e2ba07aba1890652a8723057c64a0afa50ea3fae.txt"], "foo,bar\n1,2\n3,4");
          assert.equal(testHelpers.s3.objects["bucket/csv/test/e2ba07aba1890652a8723057c64a0afa50ea3fae.csv"], "foo,bar\n1,2\n3,4\n");
          assert.equal(testHelpers.s3.objects["bucket/batch/test/1489f923c4dca729178b3e3233458550d8dddf29.csv"], "foo,bar\n1,2\n3,4\n");
        }
      );
    });

    it('should multithread from a S3Object', function() {
      testHelpers.s3.putObject({
        Bucket: "aws.lambda.us-east-1.1234567890.config",
        Key: "test.json",
        Body: "{\"Bucket\":\"bucket\"}"
      }, function() {});
      testHelpers.s3.putObject({
        Bucket: "bucket",
        Key: "csv/test/object.csv",
        Body: "foo,bar\n1,2\n3,4\n5,6\n"
      }, function() {});
      return testHelpers.assertContextSuccess(
        main.handle({S3Object: 'bucket/csv/test/object.csv', StreamName: "test", Metadata: {MultiThreading: true}}, ctx),
        ctx,
        function(results) {
          assert.deepEqual(results, { ETag: 's3-object-tag', BytesProcessed: 20 });
          assert.equal(testHelpers.s3.objects["bucket/batch/test/29e2dcfbb16f63bb0254df7585a15bb6fb5e927d.csv"], "foo,bar\n1,2\n3,4\n5,6\n");
        }
      );
    });

    it('should send CSV rows as SNS messages', function() {
      ctx.getRemainingTimeInMillis = function() { return 1001 };
      testHelpers.s3.putObject({
        Bucket: "aws.lambda.us-east-1.1234567890.config",
        Key: "test.json",
        Body: "{\"Bucket\":\"bucket\"}"
      }, function() {});
      testHelpers.s3.putObject({
        Bucket: "bucket",
        Key: "batch/test/object.csv",
        Body: "foo,bar\n1,2\n3,4\n5,6\n"
      }, function() {});
      return testHelpers.assertContextSuccess(
        main.handle({S3Object: 'bucket/batch/test/object.csv', StreamName: "test"}, ctx),
        ctx,
        function(results) {
          assert.equal(testHelpers.sns.messages.length, 3);
          assert.equal(testHelpers.sns.messages[0], "{\"foo\":\"1\",\"bar\":\"2\"}");
          assert.equal(testHelpers.sns.messages[1], "{\"foo\":\"3\",\"bar\":\"4\"}");
          assert.equal(testHelpers.sns.messages[2], "{\"foo\":\"5\",\"bar\":\"6\"}");
          assert.deepEqual(results, { ETag: 's3-object-tag', BytesProcessed: 20 });
        }
      );
    });

    it('should parse the JSON message from SNS', function() {
      ctx.getRemainingTimeInMillis = function() { return 1001 };
      testHelpers.s3.putObject({
        Bucket: "aws.lambda.us-east-1.1234567890.config",
        Key: "test.json",
        Body: "{\"Bucket\":\"bucket\"}"
      }, function() {});
      testHelpers.s3.putObject({
        Bucket: "bucket",
        Key: "batch/test/object.csv",
        Body: "foo,bar\n1,2\n3,4\n5,6\n"
      }, function() {});
      return testHelpers.assertContextSuccess(
        main.handle({Records: [{Sns: {Message: "{\"S3Object\":\"bucket/batch/test/object.csv\",\"StreamName\":\"test\"}"}}]}, ctx),
        ctx,
        function(results) {
          assert.equal(testHelpers.sns.messages.length, 3);
          assert.equal(testHelpers.sns.messages[0], "{\"foo\":\"1\",\"bar\":\"2\"}");
          assert.equal(testHelpers.sns.messages[1], "{\"foo\":\"3\",\"bar\":\"4\"}");
          assert.equal(testHelpers.sns.messages[2], "{\"foo\":\"5\",\"bar\":\"6\"}");
          assert.deepEqual(results, { ETag: 's3-object-tag', BytesProcessed: 20 });
        }
      );
    });

    it('should fail if there is no Bucket in config', function() {
      testHelpers.s3.putObject({
        Bucket: "aws.lambda.us-east-1.1234567890.config",
        Key: "test.json",
        Body: "{}"
      }, function() {});
      return testHelpers.assertContextFailure(
        main.handle({Body: 'foo,bar\n1,2\n3,4', StreamName: "test"}, ctx),
        ctx,
        /Missing S3 bucket/
      );
    });

    it('should fail if there is no StreamName in the event', function() {
      testHelpers.s3.putObject({
        Bucket: "aws.lambda.us-east-1.1234567890.config",
        Key: "test.json",
        Body: "{\"Bucket\":\"bucket\"}"
      }, function() {});
      return testHelpers.assertContextFailure(
        main.handle({Body: 'foo,bar\n1,2\n3,4'}, ctx),
        ctx,
        /Missing StreamName/
      );
    });
  });
});