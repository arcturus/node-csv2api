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
var CsvLoader = (function CsvLoader() {

  var options, csvFile, db, dbPath;

  // Parses the original input and throw errors if mandatory fields are not
  // present.
  //
  // @param {object} configOpts json containing the configuration options
  // @param {string} csvPath Full path to the csv file to parse
  var parseConfiguration = function parseConfiguration(configOpts, csvPath) {
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
      throw new Error(
        'Please specify at least one column to make it searchable');
    }

    options = configOpts;
    csvFile = csvPath;
  };

  // Creates (and empties if necessary) a new levelDB database
  //
  // @param {string} location Directoty where the db will be created
  // @param {object} json object containing levelDB creation parameters
  //      see levelDB doc for options available
  // @param {function} cb callback called once operation is done
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

  // Start with the parsing of the csv file indicated with the
  // parameter.
  //
  // @param {object} configOpts json object with needed configuration options
  // TODO: List of configurable parameters here
  // @param {string} csvPath full path to the csv file to parse
  // @param {function} cb Function to call once the process finish
  var init = function init(configOpts, csvPath, cb) {
    parseConfiguration(configOpts, csvPath);

    // So far we will leave with default configuration
    // options for levelDB
    var createDBOptions = {
      purge: !!options.clean
    };

    dbPath = './' + options.db + '.db';
    createDB(dbPath, createDBOptions, function onDB(err, leveldb) {
      if (err) {
        cb(err);
        return;
      }
      db = leveldb;
      parseCsv(cb);
    });
  };

  //
  // Once configured, insert each of the lines of the csv file into
  // the levelDB database. Generating as many key as needed for the
  // selected columns
  //
  // @param {function} cb callback called once finished.
  var parseCsv = function parseCsv(cb) {
    var cvsOptions = {};
    var ops = [];
    if (!options.cols) {
      cvsOptions.columnsFromHeader = true;
    }
    var reader = csv.createCsvFileReader(csvFile, cvsOptions);
    if (options.cols) {
      reader.setColumnNames(options.cols);
    }

    reader.addListener('data', function(data) {
      var keys = getKeys(options.keys, reader.columnNames, data);
      keys.forEach(function onKey(key) {
        ops.push({
          type: 'put',
          key: key,
          value: data,
          valueEncoding: 'json',
          keyEncoding: 'json'
        });

        if (ops.length === 100) {
          executeBatch(ops.slice(0));
          ops = [];
        }
      });
    });
    reader.addListener('end', function() {
      executeBatch(ops, function(err) {
        if (err) {
          cb(err);
        } else {
          cb(reader.columnNames);
        }
      });
    });
  };

  //
  // Execute a batch of level db operations
  //
  // @param {Array} ops Array of objects containing the add operations
  // @param {function} cb callback.
  var executeBatch = function executeBatch(ops, cb) {
    db.batch(ops, function (err) {
      if (typeof cb !== 'function') {
        return;
      }
      if (err) {
        cb(err);
      } else {
        cb();
      }
    });
  };

  //
  // Generates as many entries, with their own keys, per row
  // as needed by the number of indexed columns.
  // If the columns specified are not found in the csv file, will be discarded
  //
  // @param {Array} configCols Array of string with the columns to be searchable
  // @param {cols} cols Columns in the csv file
  // @param {Object} values Json object with the values for that row.
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

  return {
    'init': init
  };

})();


module.exports = CsvLoader;

