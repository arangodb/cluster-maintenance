/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "repair-unplanned-failover";
exports.group = "cleanup tasks";
exports.args = [
  {
    "name": "repair-unplanned-failover-file",
    "optional": false,
    "type": "jsonfile",
    "description": "json file created by analyze task"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Clears unplanned failover candidates found by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.6.1 - 3.7.99";
exports.info = `
Repairs unplanned failover candidates.
`;

exports.run = function (extra, args) {

  // imports
  const helper = require('../helper.js');
  let collections = helper.getValue("repair-unplanned-failover-file", args);

  let ns = {};
  let os = {};

  collections.forEach(function (collection) {
    let pathCurrent = "arango/Current/Collections/" + collection.dbname + "/" +
        collection.cid + "/" + collection.shname + "/failoverCandidates";
    let pathPlan = "arango/Plan/Collections/" + collection.dbname + "/" +
        collection.cid + "/shards/" + collection.shname;

    ns[pathCurrent] = collection.correct;
    os[pathCurrent] = collection.old;
    os[pathPlan] = collection.plan;
  });

  ns["arango/Plan/Version"] = {op: "increment"};

  let trx = [ ns, os ];
  print(trx);

  let res = helper.httpWrapper('POST', '/_api/agency/write', [trx]);
  print(res);
  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }
};
