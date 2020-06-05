/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "remove-dead-primaries";
exports.group = "cleanup tasks";
exports.args = [
  {
    "name": "dead-primaries-file",
    "optional": false,
    "type": "jsonfile",
    "description": "json file created by analyze task"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Removes dead primaries found by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.7.99";
exports.info = `
Removes dead primaries found by the analyze task.
`;

exports.run = function (extra, args) {

  // imports
  const fs = require('fs');
  const _ = require('underscore');
  const helper = require('../helper.js');
  let zombies = helper.getValue("dead-primaries-file", args);

  _.each(zombies, function (zombie) {
    if (zombie.database.length > 0 && zombie.primary.length > 0) {
      print("removing dead primary: " + zombie.database + "/" + zombie.primary);

      let data = {};
      data['/arango/Current/Databases/' + zombie.database + '/' + zombie.primary] = {
        'op': 'delete'
      };

      let pre = {};
      pre['/arango/Current/Databases/' + zombie.database + '/' + zombie.primary] = {
        'old': zombie.data
      };

      let res = helper.httpWrapper('POST', '/_api/agency/write', [[data, pre]]);

      if (res.results[0] === 0) {
        print("WARNING: pre-condition failed, maybe cleanup already done");
      } else {
        print("INFO: " + JSON.stringify(res));
      }
    } else {
      print("ERROR: corrupted entry in zombie file: " + JSON.stringify(zombie));
    }
  });
};
