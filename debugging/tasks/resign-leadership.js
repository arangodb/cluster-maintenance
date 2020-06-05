/* jshint globalstrict:false, strict:false, sub: true */
exports.name = "resign-leadership";
exports.group = "move shard tasks";
exports.args = [
  {
    "name": "server",
    "optional": false,
    "type": "string",
    "description": "database server"
  }
];
exports.args_arangosh = "| --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Resign leadership.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.6.0 - 3.7.99";
exports.info = `
This task resigns all leadership of a DBserver.
`;

exports.run = function (extra, args) {
  require("../helper-cleanout-server").run(extra, args, false);
};
