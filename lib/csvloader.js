'use strict';

var fs = require('fs'),
    leveldown = require('leveldown'),
    levelup = require('levelup'),
    csv = require('ya-csv');

// This module will load a csv file into a
// levelDB database.
// Will create as many keys as entries needed
// to be able to query the database by specific
// column values.
//
// For example:
//
// csv file:
// col0, col1, col2, col3
// r0v0, r0v1, r0v2, r0v3
// r1v0, r1v1, r1v2, r1v3
// ...
//
// with rXvY values for specific row and column
//
// We can specify which columns will be 'searchable'
// and for each line in the csv file we will generate 
// the following keys: (example for two searchable cols: col0 and col2)
// col0_r0v0, col2_r0v2, col0_r0v0_col2_r0v2
//
// Those three keys will contain the same value,
// the whole line and will help us to use a combination of the
// two fields.

// Constructor, validates configuration options and the existence
// of the csv file.
// The configOpts parameter is a json object that can contain
// the following fields:
//  - db: name of the levelDB database
//  - cols: array of string containing the names of the columns
//        if not present, first line will be used as column names
//  - keys: array of strings winting the columns that will be used
//      as keys.
//  - clean: bolean, removes the previous levelDB, default is true.
var CsvLoader = (function() {

  var CsvLoader = function CsvLoader(configOpts, csvPath) {
    if (!configOpts || ! csvPath) {
      throw new Error('Not enough parameters');
    }

    if (!configOpts.db) {
      throw new Error('DB name not specified');
    }

    if (!fs.existsSync(csvPath)) {
      throw new Error('Could not find ' + csvPath);
    }

    if (!configOpts.keys) {
      throw new Error('Please specify at least one column to make it searchable');
    }

    this.configOpts = configOpts;
    this.csvFile = csvPath;
  };

  var createDB = function createDB(location, options, cb) {
    // TODO: copy options to internal object with known fields
    options.valueEncoding = 'json';
    function createDB() {
      levelup(location, options, cb);
    }

    if (options && options.purge) {
      leveldown.destroy(location, createDB);
    } else {
      createDB();
    }
  };

  CsvLoader.prototype.init = function (cb) {
    // So far we will leave with default configuration
    // options for levelDB
    var options = {
      purge: !!this.configOpts.clean
    };

    this.path = './' + this.configOpts.db + '.db';
    var self = this;
    createDB(this.path, options, function onDB(err, db) {
      if (err) {
        cb(err);
        return;
      }
      self.db = db;
      self.parseCsv(cb);
    });
  };

  CsvLoader.prototype.parseCsv = function (cb) {
    var cvsOptions = {};
    var ops = [];
    if (!this.configOpts.cols) {
      cvsOptions.columnsFromHeader = true;
    }
    var reader = csv.createCsvFileReader(this.csvFile, cvsOptions);
    if (this.configOpts.cols) {
      reader.setColumnNames(this.configOpts.cols);
    }
    var self = this;
    reader.addListener('data', function (data) {
      var keys = getKeys(!self.configOpts.keys, reader.columnNames, data);
      keys.forEach(function onKey(key) {
        ops.push({
          type: 'put',
          key: key,
          value: data,
          valueEncoding: 'json',
          keyEncoding: 'json'
        });

        if (ops.length === 100) {
          executeBatch(self.db, ops.slice(0));
          ops = [];
        }
      });
    });
    reader.addListener('end', function () {
      executeBatch(self.db, ops, function(err) {
        if (err) {
          cb(err);
        } else {
          cb(reader.columnNames);
        }
      });
    });
  };

  var executeBatch = function executeBatch(db, ops, cb) {
    db.batch(ops, function (err) {
      if (typeof cb !== 'function') {
        return;
      }
      if(err) {
        cb(err);
      } else {
        cb();
      }
    });
  };

  var getKeys = function getKeys(configCols, cols, values) {
    var finalCols = [];
    if (!configCols || configCols.length === 0) {
      finalCols = cols.slice(0);
    } else {
      configCols.forEach(function onCol(c) {
        if (cols.indexOf(c)) {
          finalCols.push(c);
        }
      });
    }

    var keys = [];
    while (finalCols && finalCols.length > 0) {
      var k = finalCols.shift();
      k += '_' + values[k];
      keys.push(k);
      finalCols.forEach(function onCols(col, index) {
        var c = k + '_' + col + '_' + values[col];
        keys.push(c);
        for (var i = index + 1; i < finalCols.length; i++) {
          var z = finalCols[i] + '_' + values[finalCols[i]];
          c += '_' + z;
          keys.push(c);
        }
      });
    }
    return keys;
  };

  return CsvLoader;

})();


module.exports = CsvLoader;

