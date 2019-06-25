let file;
let target;
let leaderCid;
let shardIndex;

(function() {
  if (3 < ARGUMENTS.length) {
    file = ARGUMENTS[0];
    target = ARGUMENTS[1];
    leaderCid = ARGUMENTS[2];
    shardIndex = ARGUMENTS[3];

  } else {
    print("usage: force-failover.sh --server.endpoint LEADER-AGENT INPUT-FILE TARGET-SERVER LEADER-CID SHARD-INDEX");
    return;
  }

  try {
    let role = require("@arangodb").db._version(true).details.role;

    if (role === "AGENT") {
      let agency = arango.POST('/_api/agency/read', [
        ["/"]
      ]);

      if (agency.code === 307) {
        print("you need to connect to the leader agent");
        return;
      }
    } else {
      print("you need to connect to the leader agent, not a " + role);
      return;
    }
  } catch (e) {
    print("FATAL: " + e);
    return;
  }

  // imports
  const fs = require('fs');
  const _ = require('underscore');

  let shardGroups;

  try {
    shardGroups = JSON.parse(fs.read(file));
  } catch (e) {
    print("FATAL: " + e);
    return;
  }

  if (!shardGroups.hasOwnProperty(leaderCid)) {
    print(`FATAL: ${leaderCid} is not tracked in the output, it is not necessary to do a force failover on it, as it still has sync followers`);
    return;
  }

  const {plan, db } = shardGroups[leaderCid];
  const data = {};
  const prec = {};
  data['/arango/Plan/Version'] = {
    'op': 'increment'
  };

  for (const [cid, colInfo] of Object.entries(plan)) {
    if (colInfo.length <= shardIndex) {
      print(`FATAL: Given shardindex out of bounds, given index: ${shardIndex} numberOfShards found: ${colInfo.length}`);
      return;
    }
    const myinfo = colInfo[shardIndex];
    const planPathPrefix = `/arango/Plan/Collections/${db}/${cid}/shards/${myinfo.shard}`;
    const oldServers = myinfo.servers;
    const desiredServers = myinfo.servers
      .slice(1) // remove old leader
      .filter(s => s !== target); // remove the desired leader from the position it is in
    desiredServers.unshift(target); // add desired leader in front (inplace operation)
    data[planPathPrefix] = {
      'op': 'set',
      'new': desiredServers
    }
    prec[planPathPrefix] = {
      'old': oldServers
    }
  }

  const res = arango.POST('/_api/agency/write', JSON.stringify([[data, prec]]));
  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }
  
}());
