var fs = require('fs');
var path = require('path');

var RECORD_SIZE = 39;

function getRecords(file){
	var records = new Array();
	// fs.readFile(file, function(err, bytesRead){
	// 	if(err){
	// 		console.log(err);
	// 		return;
	// 	}
	// 	console.log("========= reading test binary file " + file +" ========");

	// 	var count = bytesRead.length / 39;
	// 	for(var readCount = 0; readCount < count; readCount++){
	// 		var record = new Object();
	// 		var tmpBuffer = bytesRead.slice(readCount * RECORD_SIZE, (readCount + 1)* RECORD_SIZE - 1);
	// 		record.longtitude = tmpBuffer.readFloatBE(0);
	// 		record.latitude = tmpBuffer.readFloatBE(4);
	// 		if(record.longtitude < 118 || record.longtitude > 123 || record.latitude < 27 || record.latitude > 32)
	// 			continue;
	// 		record.time = new Date(tmpBuffer.readInt32BE(8));	
	// 		record.isPassengerIn = (bytesRead.get(16) > 0);
	// 		record.speed = bytesRead.get(18);
	// 		record.direction = bytesRead.get(20);
	// 		records[records.length] = record;
	// 	}
	// 	// var tmpBuffer = new ()
	// 	// console.log(records);
	// 	return records;

	// });
	var bytesRead = fs.readFileSync(file);
	var count = bytesRead.length / 39;
	for(var readCount = 0; readCount < count; readCount++){
		var record = new Object();
		var tmpBuffer = bytesRead.slice(readCount * RECORD_SIZE, (readCount + 1)* RECORD_SIZE - 1);
		record.longtitude = tmpBuffer.readFloatBE(0);
		record.latitude = tmpBuffer.readFloatBE(4);
		if(record.longtitude < 118 || record.longtitude > 123 || record.latitude < 27 || record.latitude > 32)
			continue;
		record.time = new Date(tmpBuffer.readInt32BE(8));	
		record.isPassengerIn = (bytesRead.get(16) > 0);
		record.speed = bytesRead.get(18);
		record.direction = bytesRead.get(20);
		records[records.length] = record;
	}
	// var tmpBuffer = new ()
	// console.log(records);
	return records;

}

exports.getRecords = getRecords;
