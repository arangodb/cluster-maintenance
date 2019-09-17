/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango, db */
exports.name = "remove-skeleton-databases";
exports.group= "cleanup tasks";
exports.args = [ 
  { "name" : "remove-skeleton-database-file", "optional" : false, "type": "jsonfile", "description": "json file created by analyze task" } 
];
exports.args_arangosh = " --server.endpoint LEADER-AGENT";
exports.description = "Removes skeleton databases found by analyze task.";
exports.selfTests = ["arango", "db", "agencyConnection"];
exports.requires = "3.3.23 - 3.5.99";
exports.info = `
Removes skeleton databases.
`;

exports.run = function(extra, args) {

  // imports
  const fs = require('fs');
  const _ = require('underscore');
  const helper = require('../helper.js');

  let skeletons = helper.getValue("remove-skeleton-database-file");

  _.each(skeletons, function(skeleton) {
    if (skeleton.database.length > 0) {
      print("removing skeleton database: " + skeleton.database);

      let data = {};
      data['/arango/Plan/Databases/' + skeleton.database] = {
        'op': 'delete'
      };

      let pre = {};
      pre['/arango/Plan/Collections/' + skeleton.database] = {
        'oldEmpty': true
      };

      pre['/arango/Plan/Databases/' + skeleton.database] = {
        'old': skeleton.data
      };

      print(JSON.stringify([[data, pre]]));

      let res = helper.httpWrapper('POST', '/_api/agency/write', [[data, pre]]);

      if (res.results[0] === 0) {
        print("WARNING: pre-condition failed, maybe cleanup already done");
      } else {
        print("INFO: " + JSON.stringify(res));
      }
    } else {
      print("ERROR: corrupted entry in skeleton file: " + JSON.stringify(skeleton));
    }
  });
};
