/**
 * @author rajanisindhu
 */
var lazy = require('lazy');
var fs = require('fs');
var s = require('string');
var url = require('url');
var http = require('http');
var util = require('util');
var path = require('path');
var exec = require('child_process').exec;
var _ = require('underscore-node');
var csv = require('ya-csv');
var path = require('path');
var mongoose = require('mongoose');
var process = require('process');
var events = require('events');
var eventEmitter = new events.EventEmitter();

mongoose.connect('mongodb://localhost/stocks');
var db = mongoose.connection;

var quoteSchema = mongoose.Schema({
	symbol : String,
	description : String,
	currentPrice : Number,
	fetchDate : String,
	fetchTime : String,
	dayOpen : Number,
	dayHigh : Number,
	dayLow : Number,
	changePercentage : Number,
	fiftyTwoWeekLow : Number,
	fiftyTwoWeekHigh : Number,
	volume : Number,
	exchange : String
});

var symbol = [];
var quote_url = 'http://download.finance.yahoo.com/d/quotes.csv?s=%s&f=snl1d1t1ohgdrp4jka2x&e=.csv';
var DOWNLOAD_DIR = '.' + path.sep + 'downloads' + path.sep;
var mkdir = 'mkdir ' + DOWNLOAD_DIR;

var download_file_httpget = function(file_url, symbols, isLastFile) {
	console.log(util.format('/d/quotes.csv?s=%s\&f=snl1d1t1ohgdrp4jka2x&e=.csv', symbols));
	var options = {
		host : 'download.finance.yahoo.com',
		port : 80,
		path : util.format('/d/quotes.csv?s=%s\&f=snl1d1t1ohgdrp4jka2x&e=.csv', symbols)
	};
	var file_name = 'stockquotes-' + new Date().getTime() + '.csv';
	var file = fs.createWriteStream(DOWNLOAD_DIR + file_name);
	http.get(options, function(res) {
		res.on('data', function(data) {
			console.log('' + data);
			fs.appendFileSync(file_name, '' + data);
			persist_quotes(DOWNLOAD_DIR + file_name, true);
		}).on('end', function() {
			file.end();
			console.log(file_name + ' downloaded to ' + DOWNLOAD_DIR);
			persist_quotes(DOWNLOAD_DIR + file_name, false);
		});
	});
	
};
var process_files = function() {
	fs.readdir(DOWNLOAD_DIR, function(err, files) {
		if (err)
			throw err;
		_.each(files, function(file) {
			persist_quotes(file, false);
		});

	});
};
var exitApplication = function() {
	process.exit(0);
};
var persist_quotes = function(file, isLastFile) {
	console.log("Persisting quotes from file : " + file.toString());
	var reader = csv.createCsvFileReader(file);
	reader.on('data', function(data) {
		const SYMBOL = 0;
		const DESCRIPTION = 1;
		const CURRENT_PRICE = 2;
		const FETCH_DATE = 3;
		const FETCH_TIME = 4;
		const DAY_OPEN = 5;
		const DAY_HIGH = 6;
		const DAY_LOW = 7;
		const UNKNOWN_1 = 8;
		const UNKNOWN_2 = 9;
		const CHANGE_PERCENTAGE = 10;
		const FIFTY_TWO_WEEK_LOW = 11;
		const FIFTY_TWO_WEEK_HIGH = 12;
		const VOLUME = 13;
		const EXCHANGE = 14;

		console.log(data.join());
		var Quote = mongoose.model('Quote', quoteSchema);
		var q = new Quote({
			"symbol" : data[SYMBOL],
			"description" : data[DESCRIPTION],
			"currentPrice" : data[CURRENT_PRICE],
			"fetchDate" : data[FETCH_DATE],
			"fetchTime" : data[FETCH_TIME],
			"dayOpen" : !isValidValue(data[DAY_OPEN]) ? -99 : data[DAY_OPEN],
			"dayHigh" : !isValidValue(data[DAY_HIGH]) ? -99 : data[DAY_HIGH],
			"dayLow" : !isValidValue(data[DAY_LOW]) ? -99 : data[DAY_LOW],
			"fiftyTwoWeekLow" : !isValidValue(data[FIFTY_TWO_WEEK_LOW]) ? -99 : data[FIFTY_TWO_WEEK_LOW],
			"fiftyTwoWeekHigh" : !isValidValue(data[FIFTY_TWO_WEEK_HIGH]) ? -99 : data[FIFTY_TWO_WEEK_HIGH],
			"volume" : !isValidValue(data[VOLUME]) ? -99 : data[VOLUME],
			"exchange" : !isValidValue(data[EXCHANGE]) ? -99 : data[EXCHANGE],
			"changePercentage" : !isValidValue(data[CHANGE_PERCENTAGE]) ? -99 : parseFloat(data[CHANGE_PERCENTAGE].substring(0, data[CHANGE_PERCENTAGE].length - 1))
		});
		q.save(function(err, savedQuote) {
			if (err)
				throw err;
			console.log("Saved Quote");
			// if (isLastFile) {
				// eventEmitter.emit('exit');
			// }
		});
	});
};

eventEmitter.on('exit', function() {
	process.exit(0);
});

var isValidValue = function(data) {
	if (data === undefined || data === 'N/A') {
		return false;
	}
	return true;
};


var deleteFolderRecursive = function(path) {
  if( fs.existsSync(path) ) {
    fs.readdirSync(path).forEach(function(file,index){
      var curPath = path + "/" + file;
      if(fs.lstatSync(curPath).isDirectory()) { // recurse
        deleteFolderRecursive(curPath);
      } else { // delete file
        fs.unlinkSync(curPath);
      }
    });
    fs.rmdirSync(path);
  }
};

var fetch2 = function(){
	fs.createReadStream('nasdaqlisted.txt').on('end', function() {
	download_file_httpget(quote_url, symbol.join(","), true);
	//process_files();
	}).lines.forEach(function(line){
		if (!(s(line).startsWith('Symbol') || s(line).startsWith('File'))) {
		var tokens = line.toString().split('|');
		symbol.push(tokens[0]);
		if (symbol.length % 100 == 0) {
			download_file_httpget(quote_url, symbol.join(","), false);
			symbol = [];
		}
	}
	});
};

var fetch = function() {
	fs.exists('./downloads', function(exists){
		if (!exists) {
			fs.mkdirSync('./downloads');
		} else {
			deleteFolderRecursive('./downloads');
			fs.mkdirSync('./downloads');
		}
	});
	var reader = csv.createCsvFileReader('nasdaqlisted.txt', {
		'separator' : '|',
		'quote' : '"'
	});
	reader.addListener('data', function(data) {
		console.log(data[0] + ' ' + symbol.length);
		if (data[0] !== 'Symbol') {
			symbol.push(data[0]);
			if (symbol.length % 100 == 0) {
				console.log('Symbols -- ' + symbol);
				download_file_httpget(quote_url, symbol.join(","), false);
				symbol = [];
			}
		}
	});
	
	reader.addListener('end', function() {
		symbol.splice(symbol.length - 1, 1);
		download_file_httpget(quote_url, symbol.join(","), true);
	});
};

module.exports = fetch;