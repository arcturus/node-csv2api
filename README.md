# node-csv2api

Node module to create a rest api from a csv file.

From the csv file a levelDB database will be generated for quering the content via a key.
This key could be a column of the csv file (or a combination of them, just remember that keys will need to be unique).

The api answer to simple `GET` request, and query parameters based on what columns where indexed.

[![build status](https://secure.travis-ci.org/arcturus/node-csv2api.png)](http://travis-ci.org/arcturus/node-csv2api)

## Installation

This module is installed via npm:

``` bash
$ npm install node-csv2api
```

## Example Usage

``` shell
./bin/csv2api create ./register.csv --cols email
```
This will index the rows in the `register.csv` file by the column `email`. This step just creates the database.


For serving the content just do:
``` shell
./bin/csv2api serve --cols email
```

Then go to:
`http://localhost:3000/?email=<email>`


[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/arcturus/node-csv2api/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

