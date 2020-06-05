/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango, db */
exports.name = "remove-cleaned-failovers";
exports.group= "cleanup tasks";
exports.args = [ 
  { "name" : "cleaned-failovers-file", "optional" : false, "type": "jsonfile", "description": "json file created by analyze task" } 
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Clears cleaned failover candidates found by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.7.99";
exports.info = `
Removes cleaned failover candidates found by the analyze task.
`;

exports.run = function(extra, args) {

  // imports
  const fs = require('fs');
  const _ = require('underscore');
  const helper = require('../helper.js');
  let shards = helper.getValue("cleaned-failovers-file", args);

  var trx = [];
  Object.keys(shards).forEach(function(shard) {
    var oper = {};
    var prec = {};
    oper[shard] = shards[shard][0];
    prec[shard] = {old : shards[shard][1]};
    trx.push ([oper, prec]); 
  });

  let res = helper.httpWrapper('POST', '/_api/agency/write', trx);
  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }

};
