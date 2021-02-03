/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "clear-cleanout-server";
exports.group = "cleanup tasks";
exports.args = [
  {
    "name": "server",
    "optional": false,
    "type": "string",
    "description": "database server"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Clears cleanout server list";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.6.0 - 3.8.99";
exports.info = `
Removes the cleaned out servers at '/arango/Target/CleanedServers'.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');

  // get an agency dump
  const dump = helper.getAgencyDumpFromObjectOrAgency()[0];

  // show a server list
  helper.showServers(dump);

  // clear entry in agency
  const name = helper.getValue("server", args);
  const { serverId, shortName } = helper.findServer(dump, name);

  if (!shortName) {
    helper.fatal("Unknown server '" + name + "'");
  }

  const data = [[]];
  const obj = {};

  const k = '/arango/Target/CleanedServers';
  obj[k] = { op: "erase", val: serverId };

  data[0].push(obj);

  const res = helper.httpWrapper('POST', '/_api/agency/write', data);

  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }
};
