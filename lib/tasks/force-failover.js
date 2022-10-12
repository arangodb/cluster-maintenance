/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "force-failover";
exports.group = "move shard tasks";
exports.args = [
  { "name": "input-file", "optional": false, "type": "jsonfile", "description": "json file created by analyze task" },
  { "name": "target-server", "optional": false, "type": "string", "description": "server id of desired new leader (e.g. PRMR-....)" },
  { "name": "leader-cid", "optional": false, "type": "string", "description": "collection id of collection to move leadership for" },
  { "name": "shard-index", "optional": false, "type": "string", "description": "shard id (0-based) of shard to move leadership for" }
];

exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Performs forced failover as calculated by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.10.99";
exports.info = `
Executes force failover as calculated by the analyze task.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');
  let shardGroups = helper.getValue("input-file", args);
  let target = helper.getValue("target-server", args);
  let leaderCid = helper.getValue("leader-cid", args);
  let shardIndex = helper.getValue("shard-index", args);

  if (!shardGroups.hasOwnProperty(leaderCid)) {
    print(`FATAL: ${leaderCid} is not tracked in the output, it is not necessary to do a force failover on it, as it still has sync followers`);
    return;
  }

  const {plan, db} = shardGroups[leaderCid];
  const data = {};
  const prec = {};
  data['/arango/Plan/Version'] = {
    'op': 'increment'
  };

  for (const [cid, colInfo] of Object.entries(plan)) {
    if (colInfo.length <= shardIndex) {
      print(`FATAL: Given shardIndex out of bounds, given index: ${shardIndex} numberOfShards found: ${colInfo.length}`);
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
    };
    prec[planPathPrefix] = {
      'old': oldServers
    };
  }

  const res = helper.httpWrapper('POST', '/_api/agency/write', [[data, prec]]);

  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }
};
