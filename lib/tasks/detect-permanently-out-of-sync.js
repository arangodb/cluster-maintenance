/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */
exports.name = "collect-shard-stats";
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
  const conf = helper.getAgencyDumpFromObjectOrAgency()[0];
  const planDBServers = conf.arango.Plan.DBServers;
  const planDatabases = conf.arango.Plan.Databases;
  const plannedShardMap = conf.arango.Plan.Collections;
  const currentShardMap = conf.arango.Current.Collections;
  const health = conf.arango.Supervision.Health;
  var firstRun = true;
  var mismatch = {};
  
  for (var i = 0; i < 5; ++i) {
    
    var counts = {};
    
    var databases;
    if (firstRun) {
      databases = planDatabases;
    } else {
      databases = mismatch;
    }    
    _.each(planDBServers, function (val, dbserver) {
      ip = conf.arango.Supervision.Health[dbserver].Endpoint;
      arango.reconnect(ip, "_system");
      _.each(databases, function(val, database) {
        if (!counts.hasOwnProperty(database)) {
          counts[database] = {};
        }
        
        var localDB
        if (firstRun) {
          localDB = arango.GET("/_db/" + database + "/_api/collection").result;
        } else {
          localDB = mismatch[database];
        }        
        _.each(localDB, function (shard) {
          if (!shard.name.startsWith("_statistics")) {
            let c = arango.GET("/_db/" + database + "/_api/collection/" + shard.name + "/count").count;
            if (!counts[database].hasOwnProperty(shard.name)) {
              counts[database][shard.name] = {};
            }
            counts[database][shard.name][dbserver] = c;
          }
        });
      });
    });

    if (!firstRun) {
      mismatch = {};
    }
    _.each(counts, function(val, database) {
      _.each(val, function(replica, shard) {
        let nr = Object.keys(replica).length;
        if (nr >= 1) {
          let c = replica[0];
          for (var i = 1; i < nr; ++i) {
            if (replica[i] === c) {
              if (!mismatch.hasOwnProperty(database)) {
                mismatch[database] = [];
              }
              if (!mismatch[database].hasOwnProperty(shard)) {
                mismatch[database].push({ name : shard });
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
    table.setHeading('Database', 'Shard');
    _.each(mismatch, function(shards, database) {
      _.each(shards, function(shard) {  
        table.addRow(database, shard.name);
      });
    });
    print(table.toString());
  } else {
    helper.printGood('Your cluster does not have any non-syncing shards');
  }
  
};
