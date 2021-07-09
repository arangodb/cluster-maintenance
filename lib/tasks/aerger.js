/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */

exports.name = "aerger";
exports.group = "temporary";
exports.args = [
  {
    name: "database",
    optional: true,
    type: "string",
    description: "database name to use"
  }
];
exports.args_arangosh = " --server.endpoint COORDINATOR";
exports.description = "Temporary script to make trouble";
exports.selfTests = [];
exports.requires = "3.3.23 - 3.8.99";
exports.info = `
Creates a collection with silently out of sync followers.
`;

// This script creates a collection and tries to provoke havoc.
// It will intentionally bring a shard out of sync without noticing.
// Then, it will create a constant load with multi-shard transactions
// to try to interfere with the repair and diagnostic script.

const _ = require('lodash');
let internal = require('internal');
let arangodb = require('@arangodb');
let db = arangodb.db;
let { getEndpointsByType,
      waitForShardsInSync
    } = require('@arangodb/test-helper');

let rand = internal.rand;
let time = internal.time;

let makeRandomString = function(l) {
  var r = rand();
  var d = rand();
  var s = "x";
  while (s.length < l) {
    s += r;
    r += d;
  }
  return s.slice(0, l);
}

function createCollectionWithTwoShardsSameLeaderAndFollower(database, cn) {
  db._useDatabase("_system");
  try {
    db._createDatabase(database);
  } catch (e1) {
  }
  try {
    db._useDatabase(database);
  } catch(e2) {
  }
  db._create(cn, {numberOfShards:2, replicationFactor:2});
  // Get dbserver names first:
  let health = arango.GET("/_admin/cluster/health").Health;
  let endpointMap = {};
  let mapShortToLong = {};
  let agencyLeader = "";
  for (let sid in health) {
    endpointMap[health[sid].ShortName] = health[sid].Endpoint;
    mapShortToLong[health[sid].ShortName] = sid;
    if (health[sid].Role === "Agent" && health[sid].Leading) {
      agencyLeader = health[sid].Endpoint;
    }
  }
  let plan = arango.GET("/_admin/cluster/shardDistribution").results[cn].Plan;
  let shards = Object.keys(plan);
  let coordinator = "Coordinator0001";
  let leader = plan[shards[0]].leader;
  let follower = plan[shards[0]].followers[0];
  // Make leaders the same:
  if (leader !== plan[shards[1]].leader) {
    let moveShardJob = {
      database: database,
      collection: cn,
      shard: shards[1],
      fromServer: plan[shards[1]].leader,
      toServer: leader,
      isLeader: true
    };
    let res = arango.POST("/_admin/cluster/moveShard", moveShardJob);
    let start = internal.time();
    while (true) {
      if (internal.time() - start > 120) {
        assertTrue(false, "timeout waiting for shards being in sync");
        return;
      }
      let res2 = arango.GET(`/_admin/cluster/queryAgencyJob?id=${res.id}`);
      if (res2.status === "Finished") {
        break;
      }
      internal.wait(1);
    }
    // Now we have to wait until the Plan has only one follower again, otherwise
    // the second moveShard operation can fail and thus the test would be
    // vulnerable to bad timing (as has been seen on Windows):
    start = internal.time();
    while (true) {
      if (internal.time() - start > 120) {
        assertTrue(false, "timeout waiting for shards being in sync");
        return;
      }
      plan = arango.GET("/_admin/cluster/shardDistribution").results[cn].Plan;
      if (plan[shards[1]].followers.length === 1) {
        break;
      }
      internal.wait(1);
    }
  }
  // Make followers the same:
  if (follower !== plan[shards[1]].followers[0]) {
    let moveShardJob = {
      database: database,
      collection: cn,
      shard: shards[1],
      fromServer: plan[shards[1]].followers[0],
      toServer: follower,
      isLeader: false
    };
    let res = arango.POST("/_admin/cluster/moveShard", moveShardJob);
    let start = internal.time();
    while (true) {
      if (internal.time() - start > 120) {
        assertTrue(false, "timeout waiting for shards being in sync");
        return;
      }
      let res2 = arango.GET(`/_admin/cluster/queryAgencyJob?id=${res.id}`);
      if (res2.status === "Finished") {
        break;
      }
      internal.wait(1);
    }
  }
  return { endpointMap, coordinator, leader, follower, shards, cn, mapShortToLong, agencyLeader, database };
}

function switchConnectionToCoordinator(collInfo) {
  arango.reconnect(collInfo.endpointMap[collInfo.coordinator], collInfo.database, "root", "");
}

function switchConnectionToLeader(collInfo) {
  arango.reconnect(collInfo.endpointMap[collInfo.leader], collInfo.database, "root", "");
}

function switchConnectionToFollower(collInfo) {
  arango.reconnect(collInfo.endpointMap[collInfo.follower], collInfo.database, "root", collInfo.rootPasswd);
}

function switchConnectionToAgencyLeader(collInfo) {
  arango.reconnect(collInfo.agencyLeader, collInfo.database, "root", collInfo.rootPasswd);
}

function breakCollection(collInfo) {
  switchConnectionToFollower(collInfo);
  db._useDatabase(collInfo.database);
  let badShard = collInfo.shards[0];
  let leaderId = collInfo.mapShortToLong[collInfo.leader];
  for (let i = 1; i < 10; ++i) {
    let res = arango.POST(`/_api/document/${badShard}?isSynchronousReplication=${leaderId}`,
      {Hallo: -i, s: makeRandomString(70)});
    print(res);
  }
  
  switchConnectionToCoordinator(collInfo);
}

function createCollectionLeaderAndFollower(database, cn) {
  db._useDatabase("_system");
  try {
    db._createDatabase(database);
  } catch (e1) {
  }
  try {
    db._useDatabase(database);
  } catch(e2) {
  }
  db._create(cn, {numberOfShards:1, replicationFactor:3});
  // Get dbserver names first:
  let health = arango.GET("/_admin/cluster/health").Health;
  let endpointMap = {};
  let mapShortToLong = {};
  let agencyLeader = "";
  for (let sid in health) {
    endpointMap[health[sid].ShortName] = health[sid].Endpoint;
    mapShortToLong[health[sid].ShortName] = sid;
    if (health[sid].Role === "Agent" && health[sid].Leading) {
      agencyLeader = health[sid].Endpoint;
    }
  }
  let shardDist = arango.GET("/_admin/cluster/shardDistribution").results;
  let plan = shardDist[cn].Plan;
  let shards = Object.keys(plan);
  let coordinator = "Coordinator0001";
  let leader = plan[shards[0]].leader;
  let follower = plan[shards[0]].followers[0];
  return { endpointMap, coordinator, leader, follower, shards, cn, mapShortToLong, agencyLeader, database };
}

function createLoad(collInfo, seconds) {
  switchConnectionToCoordinator(collInfo);
  db._useDatabase(collInfo.database);
  let start = internal.time();
  let coll = db._collection(collInfo.cn);
  let count = 0;
  while (internal.time() - start < seconds) {
    let l = [];
    for (i = 0; i < 100; ++i) {
      l.push({Hallo:i, s: makeRandomString(80)});
    }
    try {
      coll.insert(l);
      count += 100;
    } catch (err) {
      print("Got error on insert:", JSON.stringify(err));
    }
    if (count % 10000 === 0) {
      print("Written", count, "documents in batches of 100, remaining time:", seconds - (internal.time() - start));
    }
  }
}

function repairCollection(collInfo) {
  switchConnectionToCoordinator(collInfo);
  db._useDatabase(collInfo.database);
  print(arango.PUT_RAW("/_admin/cluster/maintenance", `"on"`));
  internal.wait(3);
  let coll = db._collection(collInfo.cn);
  let collId = coll._id;
  switchConnectionToAgencyLeader(collInfo);
  let badShard = collInfo.shards[0];
  let leaderId = collInfo.mapShortToLong[collInfo.leader];
  let followerId = collInfo.mapShortToLong[collInfo.follower];
  let field = `/arango/Plan/Collections/${collInfo.database}/${collId}/shards/${badShard}`;
  let obj = {"/arango/Plan/Version":{"op":"increment"}};
  obj[field] = [leaderId];
  arango.POST("/_api/agency/write", [[obj]]);
  internal.wait(15);
  obj[field] = [leaderId, followerId];
  arango.POST("/_api/agency/write", [[obj]]);
  switchConnectionToCoordinator(collInfo);
  print(arango.PUT_RAW("/_admin/cluster/maintenance", `"off"`));
}

exports.run = function(extra, args) {
  const helper = require('../helper.js');
  const database = helper.getValue("database", args) || "_system";
  collInfo = createCollectionLeaderAndFollower(database, "badcoll");
  createLoad(collInfo, 15);
  breakCollection(collInfo);
  print("Done, 'badcoll' in database", database, " has out of sync shards.");
}
