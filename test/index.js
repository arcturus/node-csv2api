'use strict';

var expect = require('expect.js'),
    nodeCsv2api = require('..');

describe('node-csv2api', function () {
  it('should say hello', function (done) {
    expect(nodeCsv2api()).to.equal('Hello, world');
    done();
  });
});
