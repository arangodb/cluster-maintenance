/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */
exports.name = "remove-zombie-coordinators";
exports.group= "cleanup tasks";
exports.args = [ 
  {
    "name": "zombie-coordinators-file",
    "optional": false,
    "type": "jsonfile",
    "description": "json file created by analyze task"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Removes dead coordinators found by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.7.99";
exports.info = `
Removes dead coordinators found by the analyze task.
`;

exports.run = function(extra, args) {

  // imports
  const fs = require('fs');
  const _ = require('underscore');
  const helper = require('../helper.js');
  let zombies = helper.getValue("zombie-coordinators-file", args);

  var trx = {};
  zombies.forEach(function(zombie) {
    trx['/arango/Current/Coordinators/' + zombie] = {'op': 'delete'};
  });

  let res = helper.httpWrapper('POST', '/_api/agency/write', [[trx]]);
  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }

};
