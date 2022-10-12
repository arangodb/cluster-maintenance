/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "remove-zombies";
exports.group = "cleanup tasks";
exports.args = [
  {
    "name": "zombie-file",
    "optional": false,
    "type": "jsonfile",
    "description": "json file created by analyze task"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Removes zombie collections found by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.9.99";
exports.info = `
Removes zombies found by the analyze task.
`;

exports.run = function (extra, args) {
  // imports
  const _ = require('lodash');
  const helper = require('../helper.js');
  let zombies = helper.getValue("zombie-file", args);

  _.each(zombies, function (zombie) {
    if (zombie.database.length > 0 && zombie.cid.length > 0) {
      print("removing zombie collection: " + zombie.database + "/" + zombie.cid);

      let data = {};
      data['/arango/Plan/Collections/' + zombie.database + '/' + zombie.cid] = {
        'op': 'delete'
      };

      let pre = {};
      pre['/arango/Plan/Collections/' + zombie.database + '/' + zombie.cid] = {
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
