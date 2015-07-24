var pg = require("pg");
var fs = require("fs");
var path = require("path");
var async = require("async");
var parser = require("./parser");

var conn = "postgres://justcj:vag@localhost/testdb";
//var client = new pg.Client(conn);

var NUM_CLIENTS = 8;
var clients = new Array();
for (var i=0; i<NUM_CLIENTS; i++)
    clients[i] = new pg.Client(conn);
var root = "/home/zhang/just-cj/data/201401TRK"

var total = 0, size = 55000;
var count = new Array();
for(var i=0; i<NUM_CLIENTS; i++)
    count[i] = 0;


var rollback = function(client) {
      //terminating a client connection will
      //automatically rollback any uncommitted transactions
      //so while it's not technically mandatory to call
      //ROLLBACK it is cleaner and more correct
      client.query('ROLLBACK', function() {
              client.end();
                });
};

var start = new Date();
//var client = new pg.Client(conn);

async.each(clients, function(client, callback){
    client.connect(function(err){
        if(err){
            //return console.log('could not connect to postgres', err);
            console.log('could not connect to postgres', err);
            callback("connect failed");
        }else{
            callback();
        }
    });
}, function(err){
    if(err){
        console.log('one client failed to connect');
    }else{
        process.chdir(root);
        var daydirlist = fs.readdirSync(".");
        MAINLOOP:
        for(var i in daydirlist){
            //console.log(daydirlist[i]);
            process.chdir(daydirlist[i]);
            var subdirlist = fs.readdirSync(".");
            for(var j in subdirlist){
                //console.log(subdirlist[j]);
                process.chdir(subdirlist[j]);
                var filelist = fs.readdirSync(".");
                for(var k in filelist){
                    console.log(filelist[k]);
                    var records = parser.getRecords(filelist[k]);
                    for(var l in records){
                        var id = total%NUM_CLIENTS;
                        if(count[id] == 0)
                            clients[id].query("BEGIN");
                        var point = "POINT("+records[l].longtitude+" "+records[l].latitude+")";
                        query = clients[id].query(
                                "INSERT INTO "+
                                "taxis(plate_number, location, time, "+
                                "is_passenger_in, speed, direction) "+
                                "VALUES($1, $2, $3, $4, $5, $6)", 
                                [path.basename(filelist[k]), point, records[l].time, records[l].isPassengerIn,
                                records[l].speed, records[l].direction], 
                                function(err, result){
                                    if(err) {
                                        return console.error('error running query', err);
                                    }
                                });
                        total++;
                        count[id]++;
                        if(count[id] == 5000){
                            clients[id].query("COMMIT");
                            count[id] = 0;
                        }
                        if(total >= size){
                            for(var i=0; i<NUM_CLIENTS; i++){
                                if(count[i] > 0)
                                    clients[i].query("COMMIT");
                            }
                            break MAINLOOP;
                        }
                    }          
                    //total += records.length;
                    //console.log(count);
                    //if(total >= size){
                    //    for(var i=0; i<8; i++){
                    //        if(count[i] > 0)
                    //            clients[i].query("COMMIT", console.log(total));
                    //    }
                    //    break MAINLOOP;
                    //}
                }
                process.chdir("..");
            }
            process.chdir("..");
        }
        async.each(clients, function(client, callback){
            client.on('drain', function(){
                callback();
            });
        }, function(err){
            end = new Date();
            console.log('all clients drained');
            console.log("Time resumed: "+(end-start)/1000)+"s";
            clients.forEach(function(client){
                client.end();
            });
        });
        
        setInterval(function(){
            var length = 0;
            for(var i=0; i<NUM_CLIENTS; i++){
                length += clients[i].queryQueue.length;
            }
            if(length == 0) return clearInterval(this);
            console.log("Query Num/Record Size: "+length+"/"+size);
        }, 5000);

    }
});
















//client[i].connect(function(err) {
//    if(err) {
//        return console.error('could not connect to postgres', err);
//    }
//
//    process.chdir(root);
//    var daydirlist = fs.readdirSync(".");
//    MAINLOOP:
//    for(var i in daydirlist){
//        //console.log(daydirlist[i]);
//        process.chdir(daydirlist[i]);
//        var subdirlist = fs.readdirSync(".");
//        for(var j in subdirlist){
//            //console.log(subdirlist[j]);
//            process.chdir(subdirlist[j]);
//            var filelist = fs.readdirSync(".");
//            for(var k in filelist){
//                console.log(filelist[k]);
//                var records = parser.getRecords(filelist[k]);
//                for(var l in records){
//                    if(count == 0)
//                        client[i].query("BEGIN");
//                    var point = "POINT("+records[l].longtitude+" "+records[l].latitude+")";
//                    query = client[i].query(
//                            "INSERT INTO "+
//                            "taxis(plate_number, location, time, "+
//                            "is_passenger_in, speed, direction) "+
//                            "VALUES($1, $2, $3, $4, $5, $6)", 
//                            [path.basename(filelist[k]), point, records[l].time, records[l].isPassengerIn,
//                            records[l].speed, records[l].direction], 
//                            function(err, result){
//                                if(err) {
//                                    return console.error('error running query', err);
//                                }
//                            });
//                    count++;
//                    if(count == 10000){
//                        client[i].query("COMMIT", console.log(total));
//                        count = 0;
//                    }
//                }          
//                total += records.length;
//
//                //console.log(count);
//                if(total >= size){
//                    if(count > 0)
//                        client[i].query("COMMIT", console.log(total));
//                    break MAINLOOP;
//                }
//            }
//            process.chdir("..");
//        }
//        process.chdir("..");
//    }
//
//});
//
//client[i].on('drain', function() {
//    console[i].log("drained");
//    var end = new Date();
//    console.log("Time resumed: "+(end-start)/1000);
//    client[i].end();
//});
