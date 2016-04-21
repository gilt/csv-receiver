var assert = require('assert'),
    AWS = require('aws-sdk'),
    Promise = require('bluebird'),
    sinon = require('sinon'),
    testHelpers = require('../helpers');
var csvHelper, csv;

describe('csvProcessor', function() {
  before(function() {
    sinon.stub(AWS, 'S3').returns(testHelpers.s3);
    sinon.stub(AWS, 'SNS').returns(testHelpers.sns);
    csvHelper = testHelpers.require('../lib/csvHelper');
    csv = new csvHelper.Csv();
  });

  after(function() {
    AWS.S3.restore();
    AWS.SNS.restore();
  });

  describe('#load()', function() {
    it('should pull header from the object body', function() {
      return csv.load("foo,bar\n1,2")
      .then(function(numBytes) {
        assert.equal(numBytes, 8);
        assert.deepEqual(csv.header, ["foo", "bar"]);
        assert.deepEqual(csv.body, [["1", "2"]]);
      });
    });

    it('should work with escaped headers', function() {
      return csv.load("\"foo,bar\"\n\"1,2\"")
      .then(function(numBytes) {
        assert.equal(numBytes, 10);
        assert.deepEqual(csv.header, ["foo,bar"]);
        assert.deepEqual(csv.body, [["1,2"]]);
      });
    });

    it('should work with alternate delimiter', function() {
      return csv.load("\"foo||bar\"\n\"1||2\"", null, '||')
      .then(function(numBytes) {
        assert.equal(numBytes, 11);
        assert.deepEqual(csv.header, ["foo||bar"]);
        assert.deepEqual(csv.body, [["1||2"]]);
      });
    });

    it('should pull header from the argument when given', function() {
      return csv.load("foo,bar\n1,2", ["bal", "baz"])
      .then(function(numBytes) {
        assert.equal(numBytes, 0);
        assert.deepEqual(csv.header, ["bal", "baz"]);
        assert.deepEqual(csv.body, [["foo", "bar"], ["1", "2"]]);
      });
    });

    it('should set headerString', function() {
      return csv.load("\"foo,bar\",baz")
      .then(function(numBytes) {
        assert.equal(numBytes, 14);
        assert.deepEqual(csv.headerString, "\"foo,bar\",baz\n");
        assert.deepEqual(csv.body, []);
      });
    });
  });

  describe('#addToObject()', function() {
    it('should add to the current level', function() {
      var obj = {};
      csv.addToObject(obj, "foo", "bar");
      assert.deepEqual(obj, {"foo": "bar"});
      csv.addToObject(obj, "bal", "baz");
      assert.deepEqual(obj, {"foo": "bar", "bal": "baz"});
    });

    it('should add to a nested level', function() {
      var obj = {"foo": "bar"};
      csv.addToObject(obj, "nested.two.levels1", "baz1");
      assert.deepEqual(obj, {"foo": "bar", "nested": {"two": {"levels1": "baz1"}}});
      csv.addToObject(obj, "nested.two.levels2", "baz2");
      assert.deepEqual(obj, {"foo": "bar", "nested": {"two": {"levels1": "baz1", "levels2": "baz2"}}});
    });

    ["bar", 1, null, true, undefined].forEach(function(value) {
      it('will blow away ' + typeof value + ' in lieu of objects', function() {
        var obj = {"foo": value};
        assert.equal(typeof obj.foo, typeof value);
        assert.equal(obj.foo, value);
        csv.addToObject(obj, "foo.nested", "bar");
        assert.equal(typeof obj.foo, "object");
        assert.equal(obj.foo.nested, "bar");
        assert.deepEqual(obj, {"foo": {"nested": "bar"}});
      });
    });
  });

  describe('#batchify()', function() {
    it('should work for single lines', function() {
      return csv.load("foo,bar\n1,2\n3,4\n5,6\n7,8\n9,10")
      .then(function(numRows) {
        return csv.batchify({batchSize: 1})
        .then(function(batches) {
          assert.equal(batches.length, 5);
          assert.deepEqual(batches[0], [['1', '2']]);
          assert.deepEqual(batches[1], [['3', '4']]);
          assert.deepEqual(batches[2], [['5', '6']]);
          assert.deepEqual(batches[3], [['7', '8']]);
          assert.deepEqual(batches[4], [['9', '10']]);
        });
      });
    });

    it('should work for non-even endings', function() {
      return csv.load("foo,bar\n1,2\n3,4\n5,6")
      .then(function(numRows) {
        return csv.batchify({batchSize: 2})
        .then(function(batches) {
          assert.equal(batches.length, 2);
          assert.deepEqual(batches[0], [['1', '2'], ['3', '4']]);
          assert.deepEqual(batches[1], [['5', '6']]);
        });
      });
    });

    it('should work for even endings', function() {
      return csv.load("foo,bar\n1,2\n3,4\n5,6")
      .then(function(numRows) {
        return csv.batchify({batchSize: 3})
        .then(function(batches) {
          assert.equal(batches.length, 1);
          assert.deepEqual(batches[0], [['1', '2'], ['3', '4'], ['5', '6']]);
        });
      });
    });

    it('should work for longer iterations', function() {
      return csv.load("foo,bar\n1,2\n3,4\n5,6\n10,20\n30,40\n50,60\n11,21\n31,41\n51,61")
      .then(function(numRows) {
        return csv.batchify({batchSize: 3})
        .then(function(batches) {
          assert.equal(batches.length, 3);
          assert.deepEqual(batches[0], [['1', '2'], ['3', '4'], ['5', '6']]);
          assert.deepEqual(batches[1], [['10', '20'], ['30', '40'], ['50', '60']]);
          assert.deepEqual(batches[2], [['11', '21'], ['31', '41'], ['51', '61']]);
        });
      });
    });

    it('should work beyond the end', function() {
      return csv.load("foo,bar\n1,2\n3,4\n5,6")
      .then(function(numRows) {
        return csv.batchify({batchSize: 4})
        .then(function(batches) {
          assert.equal(batches.length, 1);
          assert.deepEqual(batches[0], [['1', '2'], ['3', '4'], ['5', '6']]);
        });
      });
    });

    it('should work with numBatches', function() {
      return csv.load("foo,bar\n1,2\n3,4\n5,6")
      .then(function(numRows) {
        return csv.batchify({numBatches: 2})
        .then(function(batches) {
          assert.equal(batches.length, 2);
          assert.deepEqual(batches[0], [['1', '2'], ['3', '4']]);
          assert.deepEqual(batches[1], [['5', '6']]);
        });
      });
    });

    it('should work with numBatches beyond the end', function() {
      return csv.load("foo,bar\n1,2\n3,4\n5,6")
      .then(function(numRows) {
        return csv.batchify({numBatches: 4})
        .then(function(batches) {
          assert.equal(batches.length, 3);
          assert.deepEqual(batches[0], [['1', '2']]);
          assert.deepEqual(batches[1], [['3', '4']]);
          assert.deepEqual(batches[2], [['5', '6']]);
        });
      });
    });

    it('should work with unprocessedOnly', function() {
      return csv.load("foo,bar\n1,2\n3,4\n5,6")
      .then(function(numRows) {
        csv.currentRow = 1;
        return csv.batchify({numBatches: 2, unprocessedOnly: true})
        .then(function(batches) {
          assert.equal(batches.length, 2);
          assert.deepEqual(batches[0], [['3', '4']]);
          assert.deepEqual(batches[1], [['5', '6']]);
        });
      });
    });

    it('should fail if numBatches and batchSize are missing', function() {
      return testHelpers.assertFailure(csv.batchify({unprocessedOnly: true}), /Either batchSize or numBatches is required/);
    });
  });

  describe('#compactPath()', function() {
    it('should work on already-compact path', function() {
      assert.equal(csv.compactPath('foo.bar'), 'foo.bar');
    });

    it('should remove empty parts of the path', function() {
      assert.equal(csv.compactPath('foo..bar'), 'foo.bar');
      assert.equal(csv.compactPath('foo.bar.'), 'foo.bar');
      assert.equal(csv.compactPath('.foo.bar'), 'foo.bar');
      assert.equal(csv.compactPath('...foo...bar...'), 'foo.bar');
    });

    it('should remove spaces in the path', function() {
      assert.equal(csv.compactPath('foo. .bar'), 'foo.bar');
      assert.equal(csv.compactPath('foo. bar'), 'foo.bar');
      assert.equal(csv.compactPath('foo .bar'), 'foo.bar');
      assert.equal(csv.compactPath(' foo . bar  '), 'foo.bar');
    });
  });

  describe('#processNext()', function() {
    it('should process a csv', function() {
      testHelpers.sns.messages = [];
      return csv.load("foo,bar,baz\n1,2,3\n4,5,6")
      .then(function(numRows) {
        return csv.processNext("arn:aws:sns:us-east-1:1234567890:test")
        .then(function(numBytes) {
          assert.equal(numBytes, 6);
          assert.equal(testHelpers.sns.messages.length, 1);
          assert.deepEqual(testHelpers.sns.messages[0], "{\"foo\":\"1\",\"bar\":\"2\",\"baz\":\"3\"}");
          return csv.processNext("arn:aws:sns:us-east-1:1234567890:test")
        })
        .then(function(numBytes) {
          assert.equal(numBytes, 6);
          assert.equal(testHelpers.sns.messages.length, 2);
          assert.deepEqual(testHelpers.sns.messages[1], "{\"foo\":\"4\",\"bar\":\"5\",\"baz\":\"6\"}");
          return csv.processNext("arn:aws:sns:us-east-1:1234567890:test")
        })
        .then(function(numBytes) {
          assert.equal(numBytes, 0);
          assert.equal(testHelpers.sns.messages.length, 2);
        });
      });
    });

    it('should process a csv with an extra newline at the end', function() {
      testHelpers.sns.messages = [];
      return csv.load("foo,bar,baz\n1,2,3\n4,5,67\n")
      .then(function(numRows) {
        return csv.processNext("arn:aws:sns:us-east-1:1234567890:test")
        .then(function(numBytes) {
          assert.equal(numBytes, 6);
          assert.equal(testHelpers.sns.messages.length, 1);
          assert.deepEqual(testHelpers.sns.messages[0], "{\"foo\":\"1\",\"bar\":\"2\",\"baz\":\"3\"}");
          return csv.processNext("arn:aws:sns:us-east-1:1234567890:test")
        })
        .then(function(numBytes) {
          assert.equal(numBytes, 7);
          assert.equal(testHelpers.sns.messages.length, 2);
          assert.deepEqual(testHelpers.sns.messages[1], "{\"foo\":\"4\",\"bar\":\"5\",\"baz\":\"67\"}");
          return csv.processNext("arn:aws:sns:us-east-1:1234567890:test")
        })
        .then(function(numBytes) {
          assert.equal(numBytes, 0);
          assert.equal(testHelpers.sns.messages.length, 2);
        });
      });
    });

    it('should process a csv with a header with nested objects', function() {
      testHelpers.sns.messages = [];
      return csv.load("foo.bar1,foo.bar2,baz\n1,2,3\n4,5,6")
      .then(function(numRows) {
        return csv.processNext("arn:aws:sns:us-east-1:1234567890:test")
        .then(function(numBytes) {
          assert.equal(numBytes, 6);
          assert.equal(testHelpers.sns.messages.length, 1);
          assert.deepEqual(testHelpers.sns.messages[0], "{\"foo\":{\"bar1\":\"1\",\"bar2\":\"2\"},\"baz\":\"3\"}");
          return csv.processNext("arn:aws:sns:us-east-1:1234567890:test")
        })
        .then(function(numBytes) {
          assert.equal(numBytes, 6);
          assert.equal(testHelpers.sns.messages.length, 2);
          assert.deepEqual(testHelpers.sns.messages[1], "{\"foo\":{\"bar1\":\"4\",\"bar2\":\"5\"},\"baz\":\"6\"}");
          return csv.processNext("arn:aws:sns:us-east-1:1234567890:test")
        })
        .then(function(numBytes) {
          assert.equal(numBytes, 0);
          assert.equal(testHelpers.sns.messages.length, 2);
        });
      });
    });

    it('should not fail with an empty CSV', function() {
      testHelpers.sns.messages = [];
      return csv.load("")
      .then(function(numBytes) {
        assert.equal(numBytes, 0);
        assert.equal(testHelpers.sns.messages.length, 0);
      });
    });

    it('should not fail with a null CSV', function() {
      testHelpers.sns.messages = [];
      return csv.load(null)
      .then(function(numBytes) {
        assert.equal(numBytes, 0);
        assert.equal(testHelpers.sns.messages.length, 0);
      });
    });

    it('should not fail with an undefined CSV', function() {
      testHelpers.sns.messages = [];
      return csv.load(undefined)
      .then(function(numBytes) {
        assert.equal(numBytes, 0);
        assert.equal(testHelpers.sns.messages.length, 0);
      });
    });

    it('should not fail with an empty CSV with headers', function() {
      testHelpers.sns.messages = [];
      return csv.load("foo,bar\n")
      .then(function(numBytes) {
        assert.equal(numBytes, 8);
        assert.equal(testHelpers.sns.messages.length, 0);
      });
    });
  });

  describe('#save()', function() {
    it('should put object in S3', function() {
      testHelpers.s3.clear();
      return csv.load("foo,bar,baz\n1,2,3\n4,5,6")
      .then(function(numRows) {
        return csv.save("bucket", "key")
        .then(function(csvBody) {
          assert.equal(csvBody, "1,2,3\n4,5,6\n");
          assert.equal(testHelpers.s3.objects["bucket/key"], "foo,bar,baz\n1,2,3\n4,5,6\n");
        });
      });
    });

    it('should use alternate delimiter', function() {
      testHelpers.s3.clear();
      return csv.load("foo||bar||baz\n1||2||3\n4||5||6", null, "||")
      .then(function(numRows) {
        return csv.save("bucket", "key")
        .then(function(csvBody) {
          assert.equal(csvBody, "1||2||3\n4||5||6\n");
          assert.equal(testHelpers.s3.objects["bucket/key"], "foo||bar||baz\n1||2||3\n4||5||6\n");
        });
      });
    });

    it('should streamline the CSV to a common format', function() {
      testHelpers.s3.clear();
      return csv.load("\"foo\",\"bar,baz\"\n1,\"2,3\"\n4,5")
      .then(function(numRows) {
        return csv.save("bucket", "key")
        .then(function(csvBody) {
          assert.equal(csvBody, "1,\"2,3\"\n4,5\n");
          assert.equal(testHelpers.s3.objects["bucket/key"], "foo,\"bar,baz\"\n1,\"2,3\"\n4,5\n");
        });
      });
    });

    it('should fail if no bucket is specified', function() {
      testHelpers.s3.clear();
      return csv.load("foo,bar\n1,2")
      .then(function(numRows) {
        return testHelpers.assertFailure(csv.save("", "key"), /Missing S3 bucket/)
      })
      .then(function() {
        return testHelpers.assertFailure(csv.save(null, "key"), /Missing S3 bucket/)
      })
      .then(function() {
        return testHelpers.assertFailure(csv.save(undefined, "key"), /Missing S3 bucket/)
      });
    });

    it('should fail if no key is specified', function() {
      testHelpers.s3.clear();
      return csv.load("foo,bar\n1,2")
      .then(function(numRows) {
        return testHelpers.assertFailure(csv.save("bucket", ""), /Missing object key/)
      })
      .then(function() {
        return testHelpers.assertFailure(csv.save("bucket", null), /Missing object key/)
      })
      .then(function() {
        return testHelpers.assertFailure(csv.save("bucket", undefined), /Missing object key/)
      });
    });
  });
});