var pg = require("pg");
var fs = require("fs");
var path = require("path");
var parser = require("./parser");

var conn = "postgres://justcj:vag@localhost/testdb";
var client = new pg.Client(conn);
var root = "/home/zhang/just-cj/data/201401TRK"

var count = 0, total = 0, size = 55000;


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
var client = new pg.Client(conn);
client.connect(function(err) {
    if(err) {
        return console.error('could not connect to postgres', err);
    }

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
                client.query("BEGIN");
                for(var l in records){
                    var point = "POINT("+records[l].longtitude+" "+records[l].latitude+")";
                    query = client.query(
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
                }          
                count += records.length;
                client.query("COMMIT", console.log(count));
                //console.log(count);
                if(count >= size)
                    break MAINLOOP;
            }
            process.chdir("..");
        }
        process.chdir("..");
    }

});

client.on('drain', function() {
    console.log("drained");
    var end = new Date();
    console.log("Time resumed: "+(end-start)/1000);
    client.end();
});
