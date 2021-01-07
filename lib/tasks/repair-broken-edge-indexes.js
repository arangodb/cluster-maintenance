/* jshint globalstrict:false, strict:false, sub: true */
/* global print, db */
exports.name = "repair-broken-edge-indexes";
exports.group = "cleanup tasks";
exports.args = [
  {
    "name": "broken-edge-indexes-file",
    "optional": false,
    "type": "jsonfile",
    "description": "json file created by analyze task" }
];
exports.args_arangosh = " --server.endpoint COORDINATOR";
exports.description = "Repair broken edge indexes found by analyze task.";
exports.selfTests = ["arango", "db", "coordinatorConnection"];
exports.requires = "3.3.23 - 3.7.99";
exports.info = `
Repairs broken edge index definition found by analyze task.
`;

exports.run = function (extra, args) {

  // imports
  const helper = require('../helper.js');
  const indexes = helper.getValue("broken-edge-indexes-file", args);

  let ns = {};
  let os = {};

  Object.keys(indexes).forEach(function (pos) {
    const index = indexes[pos];

    ns[index.path] = {
      'set': "new",
      'new': index.good
    };

    os[index.path] = {
      'old': index.bad
    };
  });

  ns["arango/Plan/Version"] = {op: "increment"};

  let trx = [ ns, os ];

  let res = helper.httpWrapper('POST', '/_api/agency/write', [trx]);
  print(res);
  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }
};
