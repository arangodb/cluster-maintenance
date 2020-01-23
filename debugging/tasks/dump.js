/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango */
exports.name = "dump";
exports.group= "standalone tasks";
exports.args = [ 
  { "name" : "output-file", "optional" : false, "type": "string"},
];
exports.args_arangosh = " --server.endpoint LEADER-AGENT";
exports.description = "Dumps the agency.";
exports.selfTests = ["arango", "db", "agencyConnection"];
exports.requires = "3.3.23 - 3.5.99";
exports.info = `
Get agency dump from an agency leader.
`;

exports.run = function(extra, args) {
  // imports
  const fs = require('fs');
  const helper = require('../helper.js');

  try {
    let file = helper.getValue("output-file", args);
    let dump = helper.getAgencyDumpFromObjectOrAgency(undefined)[0];
    fs.write(file, JSON.stringify([ dump ]));
    helper.printGood("wrote agency dump to: " + file)
  } catch (ex) {
    helper.fatal("error while getting agency dump: " + ex)
  }

};
