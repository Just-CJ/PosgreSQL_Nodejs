var pg = require("pg");
var fs = require("fs");
var path = require("path");
var async = require("async");
var parser = require("./parser");

var conn = "postgres://justcj:vag@localhost/testdb";

var NUM_CLIENTS = 8;
var clients = new Array();
for (var i=0; i<NUM_CLIENTS; i++)
    clients[i] = new pg.Client(conn);
var root = "/home/zhang/just-cj/data/201401TRK"

var total = 0, size = 10000000;
var count = new Array();
for(var i=0; i<NUM_CLIENTS; i++)
    count[i] = 0;


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
        var COMMIT = 0, OVER = 0;
        var start = new Date();
        var cargo = async.cargo(function(paths, callback){
            //Process file read and insert here
            for(var i=0; i<NUM_CLIENTS; i++) // transactions begin
                clients[i].query("BEGIN");
            
            MAINLOOP:
            for(var i=0; i<paths.length; i++){
                var records = parser.getRecords(paths[i]);
                for(var l in records){
                    var id = total%NUM_CLIENTS;
                    //if(count[id] == 0)
                    //    clients[id].query("BEGIN");
                    var point = "POINT("+records[l].longtitude+" "+records[l].latitude+")";
                    var query = clients[id].query(
                        "INSERT INTO "+
                        "taxis(plate_number, location, time, "+
                        "is_passenger_in, speed, direction) "+
                        "VALUES($1, $2, $3, $4, $5, $6)", 
                        [path.basename(paths[i], '.LOG'), point, records[l].time, records[l].isPassengerIn,
                        records[l].speed, records[l].direction], 
                        function(err, result){
                            if(err) {
                                return console.error('error running query', err);
                            }
                        });
                    total++;
                    //count[id]++;
                    //index_k++;
                    if(total >= size){
                        //for(var i=0; i<NUM_CLIENTS; i++){
                        //    if(count[i] > 0){
                            // COMMIT = 1;
                        //    clients[i].query("COMMIT");
                        //    }
                        //}
                        OVER = 1;
                        break MAINLOOP;
                    //OVER = 1;
                    //break;
                    }
                    //else if(count[id] == 10000){
                    //    COMMIT = 1; 
                    //}

                }
                // after a file read
            } 
            // all file read over, commit here
            async.each(clients, function(client, callback){
                client.query("COMMIT", function(){
                    callback();
                });
            }, function(err){
                console.log("Total / Size: "+ total + " / " + size);
                if(OVER == 1){
                    var end = new Date();
                    console.log("Time consumed: "+(end-start)/1000+'s');
                    clients.forEach(function(client){ // end clients
                        client.end();
                    });
                    cargo.kill(); // end cargo queue
                }
                callback();
            });


        }, 200);
        
        // Get the file paths
        process.chdir(root);
        var daydirlist = fs.readdirSync(".");
        //MAINLOOP:
        for(var i in daydirlist){
            //console.log(daydirlist[i]);
            process.chdir(daydirlist[i]);
            var subdirlist = fs.readdirSync(".");
            for(var j in subdirlist){
                //console.log(subdirlist[j]);
                process.chdir(subdirlist[j]);
                var filelist = fs.readdirSync(".");
                for(var k in filelist){
                    if(path.extname(filelist[k]) != '.LOG') continue; 
                    var file = process.cwd()+'/'+filelist[k];
                    cargo.push(file);
                }
                process.chdir("..");
            }
            process.chdir("..");
        }


    } //else end
});
