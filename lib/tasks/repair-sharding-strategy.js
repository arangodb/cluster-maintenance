/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "repair-sharding-strategy";
exports.group = "cleanup tasks";
exports.args = [
  {
    "name": "repair-sharding-strategy-file",
    "optional": false,
    "type": "jsonfile",
    "description": "json file created by analyze task"
  },
  {
    "name": "type",
    "optional": false,
    "type": "string",
    "description": "'enterprise' or 'community'"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Clears cleaned failover candidates found by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.9.99";
exports.info = `
Repairs missing sharding strategy.
`;

exports.run = function (extra, args) {

  // imports
  const helper = require('../helper.js');
  let collections = helper.getValue("repair-sharding-strategy-file", args);
  let type = helper.getValue("type", args);

  if (type !== "enterprise" && type !== "community") {
    helper.fatal("type must be 'enterprise' or 'community', got '" + type + "'");
  }

  let ns = {};
  let os = {};

  collections.forEach(function (collection) {
    let path = "arango/Plan/Collections/" + collection.database + "/" +
        collection.cid + "/shardingStrategy";
    let namePath = "arango/Plan/Collections/" + collection.database + "/" +
        collection.cid + "/name";

    let strategy = collection.newStrategy;

    if (type === "community") {
      if (strategy === "enterprise-compat") {
        strategy = "community-compat";
      }
    }

    ns[path] = strategy;
    os[path] = {oldEmpty: true};
    os[namePath] = collection.name;
  });

  ns["arango/Plan/Version"] = {op: "increment"};

  let trx = [ ns, os ];

  let res = helper.httpWrapper('POST', '/_api/agency/write', [trx]);
  print(res);
  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }
};
