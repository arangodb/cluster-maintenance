/* jshint globalstrict:false, strict:false, sub: true */
exports.name = "show-servers";
exports.group = "analyze tasks";
exports.args = [];
exports.args_arangosh = "| --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Shows a list of all servers.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.6.0 - 3.9.99";
exports.info = `
This tasks shows all servers.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');
  const conf = helper.getAgencyDumpFromObjectOrAgency()[0];
  helper.showServers(conf, helper.getAgencyConfiguration());
};
