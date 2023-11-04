/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "increase-latest-id";
exports.group = "latest id";
exports.args = [
  {
    "name": "value",
    "optional": false,
    "type": "int"
  }
];

exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Increase the /arango/Sync/LatestID.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.11.99";
exports.info = `
Increase the entry /arango/Sync/LatestID only if it is lower than some threshold value. You NEED to restart the cluster afterwards.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');
  const value = helper.getValue("value", args);
  
  let data = [['/arango/Sync/LatestID']];
  let res = helper.httpWrapper('POST', '/_api/agency/read', data);
  const previous = res[0]["arango"]["Sync"]["LatestID"];

  if (previous >= value) {
    print("INFO: no need to bump latest id to " + value + ", because it is already at " + previous);
  } else {
    const k = '/arango/Sync/LatestID';
    let data = [[]];
    const obj = {};
    obj[k] = value;
    data[0].push(obj);

    const prec = {};
    prec[k] = { "old": previous };
    data[0].push(prec);

    print("INFO: executing write of", JSON.stringify(data));
    const writeRes = helper.httpWrapper('POST', '/_api/agency/write', data);

    if (writeRes.results[0] === 0) {
      print("WARNING: pre-condition failed, maybe latest id was modified in between");
    } else {
      print("INFO: " + JSON.stringify(writeRes));
      print("INFO: latest id bumped from " + previous + " to " + value);
    }
  }
};
