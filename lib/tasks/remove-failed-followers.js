/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "remove-failed-followers";
exports.group = "cleanup tasks";
exports.args = [
  {
    name: "collection-integrity",
    optional: false,
    type: "jsonfile",
    description: "json file created by analyze task"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Clears failed followers found by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.9.99";
exports.info = `
Removes failed followers found by the analyze task.
`;

exports.run = function (extra, args) {

  // imports
  const _ = require("lodash");
  const helper = require('../helper.js');
  const shards = helper.getValue("collection-integrity", args);

  const trx = [];

  for (const entry of shards.followerOnDeadServer) {
    if (entry.currentServers) {
      let path = `arango/Current/Collections/${entry.db}/${entry.name}/${entry.shard}/servers`;
      let oper = {};
      let prec = {};

      oper[path] = _.without(entry.currentServers, entry.server);
      prec[path] = {old: entry.currentServers};
      oper["arango/Plan/Version"] = {op: "increment"};

        trx.push([oper, prec]);
    }

    if (entry.servers) {
      path = `arango/Plan/Collections/${entry.db}/${entry.name}/shards/${entry.shard}`;
      oper = {};
      prec = {};

      oper[path] = _.without(entry.servers, entry.server);
      prec[path] = {old: entry.servers};
      oper["arango/Plan/Version"] = {op: "increment"};

      trx.push([oper, prec]);
    }
  }

  const res = helper.httpWrapper('POST', '/_api/agency/write', trx);
  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }
};
