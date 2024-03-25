/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "change-latest-id";
exports.group = "latest id";
exports.args = [
  {
    "name": "value",
    "optional": false,
    "type": "int"
  }
];

exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Change the /arango/Sync/LatestID.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.12.99";
exports.info = `
Change the entry /arango/Sync/LatestID. You NEED to restart the cluster afterwards.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');
  const value = helper.getValue("value", args);

  const data = [[]];
  const obj = {};

  const k = '/arango/Sync/LatestID';
  obj[k] = { "set": value };

  data[0].push(obj);

  print("INFO: executing write of", JSON.stringify(data));
  const res = helper.httpWrapper('POST', '/_api/agency/write', data);

  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }
};
