// imports
const request = require('@arangodb/request');
const fs = require('fs');
const _ = require('underscore');

// variables
let res = arango.POST('/_api/agency/read', [["/"]]);

const agencyPlan = res[0].arango.Plan;
let databases = agencyPlan.Databases;
let agencyCollections = agencyPlan.Collections;
let distribution = {};
const health = res[0].arango.Supervision.Health;

let leaders = {};
let followers = {};

_.each(health, function (server, key){
  if (key.substring(0, 4) === "PRMR" && server.Status === 'GOOD') {
     leaders[key] = [];
     followers[key] = [];
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
    _.each(collections, function (collection, cId) {
      // if no distributeShardsLike is set
      if ((!collection.distributeShardsLike || collection.distributeShardsLike === '') && collection.name.charAt(0) !== '_') {
        distribution[dbName].push(collection);
        
        let key = Object.keys(collection.shards);
        if (key.length === 1) {
          let shards = collection.shards[key[0]];
          if (shards.length === 2) {
            if (!leaders[shards[0]]) {
              leaders[shards[0]] = [];
            }
            leaders[shards[0]].push({
              database: dbName,
              collection: collection.name,
              shard: key[0],
              leader: shards[0],
              follower: shards[1]
            });
            if (!followers[shards[1]]) {
              followers[shards[1]] = [];
            }
            followers[shards[1]].push({
              database: dbName,
              collection: collection.name,
              shard: key[0],
              leader: shards[0],
              follower: shards[1]
            });
          }
        }
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
    
    let calc = Math.floor((totalAmount / nrServers - minAmount) * factor );
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
  _.each(shards, function (shard) {
    if (amount > 0 && shard.follower !== destination) {
      moveShard(shard, destination);
      amount--;
    }
  })
};

_.each(numberOfMoves, function (amount, dbServer) {
  moveShards(dbServer, leaders[dbServer], amount, minPositionKey);  
});

fs.write("moveShards.txt", JSON.stringify(moves));
print("Move plan written to moveShards.txt");