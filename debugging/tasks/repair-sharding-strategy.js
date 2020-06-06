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
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Clears cleaned failover candidates found by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.7.99";
exports.info = `
Repairs missing sharding strategy.
`;

exports.run = function (extra, args) {

  // imports
  const helper = require('../helper.js');
  let collections = helper.getValue("repair-sharding-strategy-file", args);

  let ns = {};
  let os = {};

  collections.forEach(function (collection) {
    let path = "arango/Plan/Collections/" + collection.database + "/" +
        collection.cid + "/shardingStrategy";

    ns[path] = collection.newStrategy;
    os[path] = {oldEmpty: true};
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