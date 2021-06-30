/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */
exports.name = "detect-permanently-out-of-sync";
exports.group = "analyze tasks";
exports.args = [];
exports.args_arangosh = " --server.endpoint COORDINATOR";
exports.description = "Shows shard statistics on database servers";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.8.99";
exports.info = `
Shows all shard statictics from all DBservers.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');

  // imports
  const _ = require('lodash');
  const AsciiTable = require('../3rdParty/ascii-table');

  // get an agency dump
        print(arango);

  var conf = helper.getAgencyDumpFromObjectOrAgency()[0];
  const agLeader = arango.getEndpoint();
  var firstRun = true;
  var mismatch = {};
  

  for (var i = 0; i < 5; ++i) {

    if (!firstRun) {
      arango.reconnect(agLeader, "_system");
      conf = helper.getAgencyDumpFromObjectOrAgency()[0];
    }

    const planDBServers = conf.arango.Plan.DBServers;
    const planDatabases = conf.arango.Plan.Databases;
    const planCollections = conf.arango.Plan.Collections;
    const currentCollections = conf.arango.Current.Collections;
    const health = conf.arango.Supervision.Health;
    var planShards = {};
    var curShards = {};
    var shardToCol = {};

    _.each(planCollections, function(database, dbname) {
      _.each(database, function(collection, cname) {
        _.each(collection.shards, function(shard, shname) {
          if (!planShards.hasOwnProperty(dbname)) {
            planShards[dbname] = {};
          }
          shardToCol[shname] = { name : collection.name, cid : cname};
          planShards[dbname][shname] = shard;
        });
      });
    });

    _.each(currentCollections, function(database, dbname) {
      _.each(database, function(collection, cname) {
        _.each(collection, function(shard, shname) {
          if (!curShards.hasOwnProperty(dbname)) {
            curShards[dbname] = {};
          }
          curShards[dbname][shname] = shard.servers;
        });
      });
    });
    
    var counts = {};

    var collections;
    if (firstRun) {
      shards = planShards;
    } else {
      shards = mismatch;
    }

    _.each(planDBServers, function (val, dbserver) {
      ip = conf.arango.Supervision.Health[dbserver].Endpoint;
      arango.reconnect(ip, "_system");
      _.each(planDatabases, function(val, database) {
        
        var localDB;
        if (firstRun) { // Get all local shards
          localDB = arango.GET("/_db/" + database + "/_api/collection").result;
        } else {        // Get only those, where counts mismatched last time around
          localDB = mismatch[database];
        }
        _.each(localDB, function (shard) {
          if (!shard.name.startsWith("_statistics") && shards[database].hasOwnProperty(shard.name)) {
            let c = arango.GET("/_db/" + database + "/_api/collection/" + shard.name + "/count").count;
            if (!counts.hasOwnProperty(database)) {
              counts[database] = {};
            }            
            if (planShards[database][shard.name].indexOf(dbserver) >= 0 &&
                curShards[database][shard.name].indexOf(dbserver) >= 0) { // still planned and in sync
              if (!counts[database].hasOwnProperty(shard.name)) {
                counts[database][shard.name] = [c];
              } else {
                counts[database][shard.name].push(c); // keep track of counts
              }
            }
          }
        });
      });
      ++i;
    });

    if (!firstRun) {
      mismatch = {};
    }
    _.each(counts, function(val, database) {
      _.each(val, function(replica, shard) {
        let nr = Object.keys(replica).length;
        if (nr > 1) {
          let c = replica[0];
          for (var i = 1; i < nr; ++i) {
            if (replica[i] === c) {
              if (!mismatch.hasOwnProperty(database)) {
                mismatch[database] = {};
              }
              if (!mismatch[database].hasOwnProperty(shard)) {
                mismatch[database][shard] = { name : shard };
                break;
              }
            }
          }
        }
      });
    });

    if (_.isEmpty(mismatch)) {
      break;
    }

    firstRun = false;

  }

  if (!_.isEmpty(mismatch)) {
    helper.printBad('Your cluster has non-syncing shards')
    const table = new AsciiTable('Non-syncing shards');
    table.setHeading('Database', 'Collection', 'Shard');
    _.each(mismatch, function(shards, database) {
      _.each(shards, function(shard) {
        table.addRow(database, shardToCol[shard.name].name, shardToCol[shard.name].cid + "/" + shard.name);
      });
    });
    print(table.toString());
  } else {
    helper.printGood('Your cluster does not have any non-syncing shards');
  }

};
