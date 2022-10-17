/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "remove-zombie-analyzer-revisions";
exports.group = "cleanup tasks";
exports.args = [
  {
    name: "zombie-analyzer-revisions-file",
    optional: false,
    type: "jsonfile",
    description: "json file created by analyze task"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Removes dead analyzer revisions found by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.7.0 - 3.10.99";
exports.info = `
Removes dead analyzer revisions found by the analyze task.
`;

exports.run = function (extra, args) {
  // imports
  const helper = require('../helper.js');
  const zombies = helper.getValue("zombie-analyzer-revisions-file", args);

  var trx = {};
  zombies.forEach(function (zombie) {
    trx['/arango/Plan/Analyzers/' + zombie] = {op: 'delete'};
  });

  const res = helper.httpWrapper('POST', '/_api/agency/write', [[trx]]);
  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }

};
