var assert = require('assert'),
    Promise = require('bluebird'),
    csvProcessor = require('../../lib/csvProcessor.js');

describe('csvProcessor', function() {
  describe('#addToObject()', function() {
    it('should add to the current level', function() {
      var obj = {};
      csvProcessor.addToObject(obj, "foo", "bar");
      assert.deepEqual(obj, {"foo": "bar"});
      csvProcessor.addToObject(obj, "bal", "baz");
      assert.deepEqual(obj, {"foo": "bar", "bal": "baz"});
    });

    it('should add to a nested level', function() {
      var obj = {"foo": "bar"};
      csvProcessor.addToObject(obj, "nested.two.levels1", "baz1");
      assert.deepEqual(obj, {"foo": "bar", "nested": {"two": {"levels1": "baz1"}}});
      csvProcessor.addToObject(obj, "nested.two.levels2", "baz2");
      assert.deepEqual(obj, {"foo": "bar", "nested": {"two": {"levels1": "baz1", "levels2": "baz2"}}});
    });

    ["bar", 1, null, true, undefined].forEach(function(value) {
      it('will blow away ' + typeof value + ' in lieu of objects', function() {
        var obj = {"foo": value};
        assert.equal(typeof obj.foo, typeof value);
        assert.equal(obj.foo, value);
        csvProcessor.addToObject(obj, "foo.nested", "bar");
        assert.equal(typeof obj.foo, "object");
        assert.equal(obj.foo.nested, "bar");
        assert.deepEqual(obj, {"foo": {"nested": "bar"}});
      });
    });
  });

  describe('#compactPath()', function() {
    it('should work on already-compact path', function() {
      assert.equal(csvProcessor.compactPath('foo.bar'), 'foo.bar');
    });

    it('should remove empty parts of the path', function() {
      assert.equal(csvProcessor.compactPath('foo..bar'), 'foo.bar');
      assert.equal(csvProcessor.compactPath('foo.bar.'), 'foo.bar');
      assert.equal(csvProcessor.compactPath('.foo.bar'), 'foo.bar');
      assert.equal(csvProcessor.compactPath('...foo...bar...'), 'foo.bar');
    });

    it('should remove spaces in the path', function() {
      assert.equal(csvProcessor.compactPath('foo. .bar'), 'foo.bar');
      assert.equal(csvProcessor.compactPath('foo. bar'), 'foo.bar');
      assert.equal(csvProcessor.compactPath('foo .bar'), 'foo.bar');
      assert.equal(csvProcessor.compactPath(' foo . bar  '), 'foo.bar');
    });
  });

  describe('#processCsv()', function() {
    it('should process a csv', function() {
      var messages = [];
      return csvProcessor.processCsv("foo,bar,baz\n1,2,3\n4,5,6", function(message) {
        messages.push(message);
      }).
      then(function () {
        assert.equal(messages.length, 2);
        assert.deepEqual(messages[0], {"foo": "1", "bar": "2", "baz": "3"});
        assert.deepEqual(messages[1], {"foo": "4", "bar": "5", "baz": "6"});
      });
    });

    it('should process a csv with an extra newline at the end', function() {
      var messages = [];
      return csvProcessor.processCsv("foo,bar,baz\n1,2,3\n4,5,6\n", function(message) {
        messages.push(message);
      }).
      then(function () {
        assert.equal(messages.length, 2);
        assert.deepEqual(messages[0], {"foo": "1", "bar": "2", "baz": "3"});
        assert.deepEqual(messages[1], {"foo": "4", "bar": "5", "baz": "6"});
      });
    });

    it('should process a csv with a header with nested objects', function() {
      var messages = [];
      return csvProcessor.processCsv("foo.bar1,foo.bar2,baz\n1,2,3\n4,5,6", function(message) {
        messages.push(message);
      }).
      then(function () {
        assert.equal(messages.length, 2);
        assert.deepEqual(messages[0], {"foo": {"bar1": "1", "bar2": "2"}, "baz": "3"});
        assert.deepEqual(messages[1], {"foo": {"bar1": "4", "bar2": "5"}, "baz": "6"});
      });
    });

    it('should not fail with an empty CSV', function() {
      var messages = [];
      return csvProcessor.processCsv("", function(message) {
        messages.push(message);
      }).
      then(function () {
        assert.equal(messages.length, 0);
      });
    });

    it('should not fail with a null CSV', function() {
      var messages = [];
      return csvProcessor.processCsv(null, function(message) {
        messages.push(message);
      }).
      then(function () {
        assert.equal(messages.length, 0);
      });
    });

    it('should not fail with an undefined CSV', function() {
      var messages = [];
      return csvProcessor.processCsv(undefined, function(message) {
        messages.push(message);
      }).
      then(function () {
        assert.equal(messages.length, 0);
      });
    });

    it('should not fail with an empty CSV with headers', function() {
      var messages = [];
      return csvProcessor.processCsv("foo,bar", function(message) {
        messages.push(message);
      }).
      then(function () {
        assert.equal(messages.length, 0);
      });
    });
  });
});