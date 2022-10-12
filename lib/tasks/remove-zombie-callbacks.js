/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "remove-zombie-callbacks";
exports.group = "cleanup tasks";
exports.args = [
  {
    "name": "zombie-callback-file",
    "optional": false,
    "type": "jsonfile",
    "description": "json file created by analyze task"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Removes zombie callbacks found by analyze task.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.10.99";
exports.info = `
Removes zombies callbacks found by the analyze task.
`;

exports.run = function (extra, args) {
  // imports
  const helper = require('../helper.js');
  let zombies = helper.getValue("zombie-callback-file", args);
  let data = [];

  Array.prototype.forEach.call(zombies, zombie => {
    let trx = {};
    trx[Object.values(zombie)[0]] = {"op": "unobserve", "url": Object.keys(zombie)[0]};
    data.push([trx]);
  });

  let res = helper.httpWrapper('POST', '/_api/agency/write', data);

  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }

};
