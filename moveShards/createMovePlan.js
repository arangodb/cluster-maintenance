let file;

if (0 < ARGUMENTS.length) {
  file = ARGUMENTS[0];
}

// imports
const request = require('@arangodb/request');
const fs = require('fs');
const _ = require('underscore');

// variables
let dump;

if (file) {
  dump = JSON.parse(fs.read(file));

  if (Array.isArray(dump)) {
    dump = dump[0];
  } else {
    dump = dump.agency;
  } 
} else {
  dump = arango.POST('/_api/agency/read', [
    ["/"]
  ])[0];
}

const agencyPlan = dump.arango.Plan;
const databases = agencyPlan.Databases;
const agencyCollections = agencyPlan.Collections;
const health = dump.arango.Supervision.Health;

let distribution = {};
let leaders = {};
let followers = {};

_.each(health, function(server, dbServer) {
  if (dbServer.substring(0, 4) === "PRMR" && server.Status === 'GOOD') {
    leaders[dbServer] = [];
    followers[dbServer] = [];
  }
});

// prepare shard lists
_.each(databases, function(val, dbName) {
  if (dbName.charAt(0) !== '_') {
    distribution[dbName] = [];
  }
});

_.each(agencyCollections, function(collections, dbName) {
  if (dbName.charAt(0) !== '_') {
    _.each(collections, function(collection, cId) {
      let cName = collection.name;

      // if distributeShardsLike is set, ignore this entry
      if ((!collection.distributeShardsLike || collection.distributeShardsLike === '') && cName && cName.charAt(0) !== '_') {
        distribution[dbName].push(collection);

        _.each(collection.shards, function(shard, sName) {
          if (0 < shard.length) {
            for (let i = 0; i < shard.length; ++i) {
              let dbServer = shard[i];

              if (i == 0) {
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
  }
});

let minAmount;

let totalAmount = 0;
let minPositionKey;
let nrServers = 0;

_.each(leaders, function(server, key) {
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
_.each(leaders, function(server, key) {
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
_.each(numberOfMoves, function(amount, key) {
  print("Key: " + key + " number of shards to move: " + amount);
});

let moves = [];
let moveShard = function(shard, destination) {
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

let moveShards = function(dbServer, shards, amount, destination) {
  _.each(shards, function(shard) {
    if (amount > 0 && !_.contains(shard.followers, destination)) {
      moveShard(shard, destination);
      amount--;
    }
  });
};

_.each(numberOfMoves, function(amount, dbServer) {
  moveShards(dbServer, leaders[dbServer], amount, minPositionKey);
});

fs.write("moveShards.txt", JSON.stringify(moves));
print("Move plan written to moveShards.txt");
