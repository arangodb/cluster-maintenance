/* jshint globalstrict:false, strict:false, sub: true */
exports.name = "history";
exports.group = "standalone tasks";
exports.args = [ 
  { "name" : "output-file", "optional" : false, "type": "string"},
];
exports.args_arangosh = " --server.endpoint COORDINATOR";
exports.description = "Get agency-history from an agent.";
exports.selfTests = ["arango", "db", "coordinatorConnection"];
exports.requires = "3.3.23 - 3.7.99";
exports.info = `
Get agency-history from an coordinator.
`;

exports.run = function(extra, args) {
  // imports
  const fs = require('fs');
  const helper = require('../helper.js');

  try {
    let file = helper.getValue("output-file", args);
    let history = helper.getAgencyHistoryFromCoordinator();
    fs.write(file, JSON.stringify(history));
    helper.printGood("wrote agency history to: " + file)
  } catch (ex) {
    helper.fatal("error while getting agency history: " + ex)
  }
};
