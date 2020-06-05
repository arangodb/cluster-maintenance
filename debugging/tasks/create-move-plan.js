/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */
exports.name = "create-move-plan (deprecated)";
exports.group = "move shard tasks";
exports.args = [
  {
    "name": "dump-file",
    "optional": true,
    "type": "jsonfile",
    "description": "agency dump file"
  }
];
exports.args_arangosh = "| --server.endpoint LEADER-AGENT";
exports.description = "Creates plan to rebalance shards in your cluster.";
exports.selfTests = ["arango", "db"];
exports.requires = "3.3.23 - 3.6.99";
exports.info = `
This task creates operations that can be applied to rebalance shards in a
cluster that has become inbalanced due to server failures.

Note: This task has the implicit assumption that there is one
DBServer with few shards and two with many (e.g. 1000, 1000, 10000). 
If there are two DBServers with lower than average
number of shards, you would need to run the task twice.

Currently there is a limit of 50k move shard jobs set (due to
JavaScript String limitations).
`;

// TODO
// - It has happened that all shard-leaders for a collection have been moved to the same server
// - It looks like only leader might are rebalanced not followers

exports.run = function (extra, args) {

  // imports
  const helper = require('../helper.js');
  const fs = require('fs');
  const _ = require('underscore');

  // variables
  const file = helper.getValue("dump-file", args);
  const dump = helper.getAgencyDumpFromObjectOrAgency(file)[0];
  const agencyPlan = dump.arango.Plan;
  const databases = agencyPlan.Databases;
  const agencyCollections = agencyPlan.Collections;
  const health = dump.arango.Supervision.Health;

  let distribution = {};
  let leaders = {};
  let followers = {};

  _.each(health, function (server, dbServer) {
    if (dbServer.substring(0, 4) === "PRMR" && server.Status === 'GOOD') {
      leaders[dbServer] = [];
      followers[dbServer] = [];
    }
  });

  // prepare shard lists
  _.each(databases, function (val, dbName) {
    distribution[dbName] = [];
  });

  _.each(agencyCollections, function (collections, dbName) {
    _.each(collections, function (collection, cId) {
      let cName = collection.name;

      // if distributeShardsLike is set, ignore this entry
      if ((!collection.distributeShardsLike || collection.distributeShardsLike === '') && cName && cName.charAt(0) !== '_') {
        distribution[dbName].push(collection);

        _.each(collection.shards, function (shard, sName) {
          if (shard.length > 0) {
            for (let i = 0; i < shard.length; ++i) {
              let dbServer = shard[i];

              if (i === 0) {
                if (!leaders[dbServer]) {
                  leaders[dbServer] = [];
                }

                leaders[dbServer].push({
                  database: dbName,
                  collection: cName,
                  shard: sName,
                  leader: dbServer,
                  followers: _.clone(shard).shift()
                });
              } else {
                if (!followers[dbServer]) {
                  followers[dbServer] = [];
                }

                followers[dbServer].push({
                  database: dbName,
                  collection: cName,
                  shard: sName,
                  leader: shard[0],
                  follower: dbServer
                });
              }
            }
          }
        });
      }
    });
  });

  let minAmount;

  let totalAmount = 0;
  let minPositionKey;
  let nrServers = 0;

  _.each(leaders, function (server, key) {
    nrServers++;
    let nrLeaders = server.length;
    let nrFollowers = followers[key].length;

    if (nrLeaders + nrFollowers < minAmount || minAmount === undefined) {
      minAmount = nrLeaders + nrFollowers;
      minPositionKey = key;
    }
    print("Number of leaders/followers on server " + key + ": " + server.length + "/" + followers[key].length);

    totalAmount += (nrLeaders + nrFollowers);
  });

  print("Total amount: " + totalAmount);
  print("Number of servers: " + nrServers);
  print("Smallest server: " + minPositionKey);
  print("Smallest amount: " + minAmount);

  let numberOfMoves = {};
  _.each(leaders, function (server, key) {
    if (key !== minPositionKey) {
      let factor = leaders[key].length / totalAmount;

      let calc = Math.floor((totalAmount / nrServers - minAmount) * factor);
      if (calc < 0) {
        calc = 0;
      }
      numberOfMoves[key] = calc;
    }
  });

  print("Number of shards to move: ");
  _.each(numberOfMoves, function (amount, key) {
    print("Key: " + key + " number of shards to move: " + amount);
  });

  let moves = [];
  let moveShard = function (shard, destination) {
    if (moves.length < 50000) {
      moves.push({
        database: shard.database,
        collection: shard.collection,
        shard: shard.shard,
        fromServer: shard.leader,
        toServer: destination
      });
    }
  };

  let moveShards = function (dbServer, shards, amount, destination) {
    _.each(shards, function (shard) {
      if (amount > 0 && !_.contains(shard.followers, destination)) {
        moveShard(shard, destination);
        amount--;
      }
    });
  };

  _.each(numberOfMoves, function (amount, dbServer) {
    moveShards(dbServer, leaders[dbServer], amount, minPositionKey);
  });

  fs.write("moveShards.json", JSON.stringify(moves));
  print("Move plan written to moveShards.json");
};
