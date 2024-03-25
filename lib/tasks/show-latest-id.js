/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "show-latest-id";
exports.group = "latest id";
exports.args = [
];

exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Show the /arango/Sync/LatestID.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.12.99";
exports.info = `
Read the entry /arango/Sync/LatestID.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');
  const data = [['/arango/Sync/LatestID']];

  const res = helper.httpWrapper('POST', '/_api/agency/read', data);

  print("INFO: latest id is: " + res[0]["arango"]["Sync"]["LatestID"]);
};
