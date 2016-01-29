var Promise = require('bluebird');
var fs = Promise.promisifyAll(require('fs'));
var request = Promise.promisifyAll(require('request'));
var nodemailer = Promise.promisifyAll(require('nodemailer'));
var moment = require('moment');
var talib = require('node-talib');

function get_price(symbol) {
	var options = {
		'url': 'http://finance.yahoo.com/webservice/v1/symbols/' + symbol + '/quote?format=json',
		'json': true
	};

	return request.getAsync(options)
		.then(function (res) {
			return parseFloat(res['body']['list']['resources'][0]['resource']['fields']['price']);
		});
}

function get_historical_data(symbol, start, end) {
	var qs = {
		'q': 'select * from yahoo.finance.historicaldata where symbol = "' + symbol + '" and startDate = "' + start + '" and endDate = "' + end + '"',
		'format': 'json',
		'env': 'store://datatables.org/alltableswithkeys',
		'callback': ''
	};

	var options = {
		'url': 'https://query.yahooapis.com/v1/public/yql',
		'qs': qs,
		'json': true
	};

	return request.getAsync(options)
		.then(function (res) {
			var results = res['body']['query']['results']['quote'];

			results.sort(function (a, b) {
				return moment(a['Date']).unix() - moment(b['Date']).unix();
			});

			return build_market_data(results);
		});
}

function build_market_data(rows) {
	var market_data = {
		'open': [],
		'close': [],
		'high': [],
		'low': [],
		'volume': []
	};

	rows.forEach(function (row) {
		market_data['open'].push(row['Open']);
		market_data['close'].push(row['Adj_Close']);
		market_data['high'].push(row['High']);
		market_data['low'].push(row['Low']);
		market_data['volume'].push(row['Volume']);
	});

	return market_data;
}

function execute_ta_command(options) {
	return new Promise(function (resolve, reject) {
		talib.execute(options, function (res) {
			resolve(res);
		});
	});
}

function calculate_rsi(market_data) {
	var options = {
		'name': 'RSI',
		'startIdx': 0,
		'endIdx': market_data['close'].length - 1,
		'inReal': market_data['close'],
		'optInTimePeriod': 14
	};

	return execute_ta_command(options)
		.then(function (res) {
			return res['result']['outReal'].pop();
		});
}

function calculate_macd(market_data) {
	var options = {
		'name': 'MACD',
		'startIdx': 0,
		'endIdx': market_data['close'].length - 1,
		'inReal': market_data['close'],
		'optInFastPeriod': 12,
		'optInSlowPeriod': 26,
		'optInSignalPeriod': 9
	};

	return execute_ta_command(options)
		.then(function (res) {
			return res['result']['outMACD'].pop();
		});
}

function send_email(from, to, subject, body, password) {
	var transporter = nodemailer.createTransport('smtps://' + from + '%40gmail.com:' + password + '@smtp.gmail.com');

	var options = {
		'from': from,
		'to': to,
		'subject': subject,
		'text': body
	};

	return transporter.sendMail(options);
}

function handle_triggers(job, price, macd, rsi) {
	return Promise.resolve(job['triggers']).each(function (trigger) {
		var should_trigger = eval(trigger['rule']);

		var body = '';
		body += 'Symbol: ' + job['symbol'] + '\n';
		body += 'Rule: ' + trigger['rule'] + '\n';
		body += 'Price: ' + price + '\n';
		body += 'RSI: ' + rsi + '\n';
		body += 'MACD: ' + macd + '\n';

		if (should_trigger) {
			return send_email(job['from'], trigger['email'], trigger['subject'], body, job['password']);
		}
	});
}

function read_file(name) {
	return fs.readFileAsync(name)
		.then(function (res) {
			return JSON.parse(res);
		});
}

function handle_job(job) {
	return Promise.all([
		get_historical_data(job['symbol'], moment().subtract(job['backlog'], 'days').format('YYYY-MM-DD'), moment().format('YYYY-MM-DD')),
		get_price(job['symbol'])
	])
	.then(function (res) {
		var market_data = res[0];
		var price = res[1];

		market_data['close'].push(price);

		return Promise.all([
			calculate_macd(market_data),
			calculate_rsi(market_data)
		])
		.then(function (res) {
			var macd = res[0];
			var rsi = res[1];

			return handle_triggers(job, price, macd, rsi);
		});
	})
	.then(function () {
		return Promise.delay(job['delay']);
	});
}

loop();