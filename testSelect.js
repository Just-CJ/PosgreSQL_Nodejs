pg = require("pg");
var async = require("async");
var conn = "postgres://justcj:vag@localhost/testdb";

var client = new pg.Client(conn);

client.connect(function(err){
    if(err){
        console.log('could not connect to postgres', err);
    }else{
        async.series([
            function(callback){
                //query case 1
                var start = new Date();
                client.query("SET enable_bitmapscan TO off");
                client.query("SET enable_indexscan TO off");
                console.log("test case 1:");
                client.query("SELECT plate_number, time, location, ST_Distance(location, 'SRID=4326;POINT(120.83 27.93)'::geometry) as d from taxis WHERE ST_DWithin(location, 'POINT(120.83 27.93)', 500) ORDER BY d limit 1000", 
                    function(err, result){
                        if(err){
                            console.log("query error!");
                            callback();
                        }else{
                            var end = new Date();
                            console.log("Time consumed: "+(end-start)/1000+"s");
                            callback();
                        }
                    });

            }, 
            function(callback){
                //query case 2
                callback();
            }], 
            function(err, results){
                // test over
                console.log("Test Over!");
                client.end();
            });

    }
});
