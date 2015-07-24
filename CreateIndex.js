// Create Index
var pg = require("pg");
var async = require("async");

var conn = "postgres://justcj:vag@localhost/testdb";

var clients2 = new Array();
for(var i=0; i<3; i++)
clients2[i] = new pg.Client(conn);

async.each(clients2, function(client, callback){
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
        var start2 = new Date();
        async.parallel([
            function(callback){
                var start = new Date();
                clients2[0].query("create index on taxis(plate_number)", function(err, result){
                    if(err)
                    return console.error('error running query', err);
                var end = new Date();
                console.log("Time spent on plate_number index: "+(end-start)/1000+"s");
                callback(null, 'one');
                });
            },
            function(callback){
                var start = new Date();
                clients2[1].query("create index on taxis(time)", function(err, result){
                    if(err)
                    return console.error('error running query', err);
                var end = new Date();
                console.log("Time spent on time index: "+(end-start)/1000+"s");
                callback(null, 'two');
                });
            }, 
            function(callback){
                var start = new Date();
                clients2[2].query("create index on taxis using gist(location)", function(err, result){
                    if(err)
                    return console.error('error running query', err);
                var end = new Date();
                console.log("Time spent on location index: "+(end-start)/1000+"s");
                callback(null, 'three');
                });
            }], function(err, result){
                var end = new Date();
                console.log("Time spent on all index: "+(end-start2)/1000+"s");
                //clients.forEach(function(client){
                //    client.end();
                //});

                clients2.forEach(function(client){
                    client.end();
                });

            });
    }
});

