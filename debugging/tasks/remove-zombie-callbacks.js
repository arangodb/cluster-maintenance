/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango, db */
exports.name = "remove-zombie-callbacks";
exports.group= "cleanup tasks";
exports.args = [ 
  { "name" : "zombie-callback-file", "optional" : false, "type": "jsonfile", "description": "json file created by analyze task" } 
];
exports.args_arangosh = " --server.endpoint LEADER-AGENT";
exports.description = "Removes zombie callbacks found by analyze task.";
exports.selfTests = ["arango", "db", "agencyConnection"];
exports.requires = "3.3.23 - 3.5.99";
exports.info = `
Removes zombies callbacks found by the analyze task.
`;

exports.run = function(extra, args) {
  // imports
  const _ = require('underscore');
  const helper = require('../helper.js');
  let zombies = helper.getValue("zombie-callback-file",args);
  let data = [];

  Array.prototype.forEach.call(zombies, zombie => {
    let trx = {};
    trx[Object.values(zombie)[0]] = {"op": "unobserve", "url":Object.keys(zombie)[0]};
    data.push([trx]);
  });

  let res = helper.httpWrapper('POST', '/_api/agency/write', data);

  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }

};
