'use strict';

// Module to create a rest api from a csv file.
// It will receive parameters to 'simulate' indexes
// to allow search by the columns of the csv document.
// The storage is supported via levelDB

var express = require('express'),
    levelup = require('levelup'),
    csvloader = require('./lib/csvloader');

var csv2api = (function csv2api() {

  var config, db;

  var create = function create(options, csvPath, cb) {
    cvsloader.init(options, csvPath, cb);
  };

  var init = function init(cols, dbPath, port) {
    if (!cols || !dbPath) {
      throw new Error('Insufficient parameters, please check documentation');
    }

    config = {
      'port': port || 3000,
      'dbPath': dbPath,
      'cols': cols
    };

    initLevelDB();
    serve();
  };

  var serve = function serve() {
    var app = express();

    app.get('/', handler);
    app.get('/version', version);

    app.listen(config.port);
  };

  var handler = function handler(req, res) {
    var key = buildKey(req);

    if (key === null) {
      res.send(404, 'Invalid parameters, try with ' +
        JSON.stringify(config.cols));
      return;
    }

    queryLevelDB(key, res);
  };

  var buildKey = function buildKey(req) {
    var key = [];

    config.cols.forEach(function onCol(col) {
      if (req.query[col]) {
        key.push(col + '_' + req.query[col]);
      }
    });

    return key.length === 0 ? null : key.join('_');
  };

  var queryLevelDB = function queryLevelDB(key, res) {
    db.get(key, function onResponse(err, value) {
      if (err) {
        res.send(404, err);
      } else {
        res.send(200, value);
      }
    });
  };

  var initLevelDB = function initLevelDB() {
    if (!config.dbPath) {
      throw new Error('Unkonwn leveldb path');
    }
    db = levelup(config.dbPath, {keyEncoding: 'json', valueEncoding: 'json'});
  };

  var version = function version(req, res) {
    var pkg = require('./package.json');
    res.send(200, 'Version: ' + pkg.version);
  };

  return {
    'init': init,
    'create': create
  };

})();

module.exports = csv2api;
