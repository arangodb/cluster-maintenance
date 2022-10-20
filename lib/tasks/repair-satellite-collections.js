/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "repair-satellite-collections";
exports.group = "cleanup tasks";
exports.args = [
  {
    "name": "repair-satellite-collections-file",
    "optional": false,
    "type": "jsonfile",
    "description": "json file created by analyze task"
  },
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Repair satellite collection issues found by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.10.99";
exports.info = `
Repairs satellite collection replication factor.
`;

exports.run = function (extra, args) {

  // imports
  const helper = require('../helper.js');
  let collections = helper.getValue("repair-satellite-collections-file", args);

  let ns = {};
  let os = {};

  collections.forEach(function (collection) {
    let path = "arango/Plan/Collections/" + collection.database + "/" +
        collection.cid + "/replicationFactor";
    let namePath = "arango/Plan/Collections/" + collection.database + "/" +
        collection.cid + "/name";

    ns[path] = "satellite";
    os[path] = {old: 0};
    os[namePath] = collection.collection;
  });

  ns["arango/Plan/Version"] = {op: "increment"};

  let trx = [ ns, os ];

  print(trx);
  let res = helper.httpWrapper('POST', '/_api/agency/write', [trx]);
  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }
};
