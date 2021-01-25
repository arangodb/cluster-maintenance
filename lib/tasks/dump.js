/* jshint globalstrict:false, strict:false, sub: true */
exports.name = "dump";
exports.group = "standalone tasks";
exports.args = [
  {
    "name": "output-file",
    "optional": false,
    "type": "string"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Gets agency-dump from an agent.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.7.99";
exports.info = `
Gets agency-dump from an agency leader.
`;

exports.run = function (extra, args) {
  // imports
  const fs = require('fs');
  const helper = require('../helper.js');

  try {
    let file = helper.getValue("output-file", args);
    let dump = helper.getAgencyDumpFromObjectOrAgency()[0];
    fs.write(file, JSON.stringify([ dump ]));
    helper.printGood("wrote agency dump to: " + file);
  } catch (ex) {
    helper.fatal("error while getting agency dump: " + ex);
  }
};
