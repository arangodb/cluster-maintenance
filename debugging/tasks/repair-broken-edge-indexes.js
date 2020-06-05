/* jshint globalstrict:false, strict:false, sub: true */
/* global print, db */
exports.name = "repair-broken-edge-indexes";
exports.group= "cleanup tasks";
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

exports.run = function(extra, args) {

  // imports
  const fs = require('fs');
  const _ = require('underscore');
  const helper = require('../helper.js');
  const indexes = helper.getValue("broken-edge-indexes-file", args);

  Object.keys(indexes).forEach(function(pos) {
    const index = indexes[pos];
    try {
      const result = db._executeTransaction({
        params: {path: index.path, good: index.good},
        action: p => {return global.ArangoAgency.set(p.path, p.good);},
        collections: {read:[], write: []}
      });

      if (result) {
        print("INFO: fixed " + index.path);
      } else {
        print("WARNING: failed to fixed " + index.path);
      }
    } catch (ex) {
      print("ERROR: failed to fixed " + index.path + ": " + ex);
    }
  });

  const plan = db._executeTransaction({action: () => {
    const v = global.ArangoAgency.get("/Plan/Version").arango.Plan.Version;
    global.ArangoAgency.set("/Plan/Version", v + 1);
    return global.ArangoAgency.get("/Plan/Version").arango.Plan.Version;
  }, collections: {read:[], write: []}});
  print("INFO: new plan version " + JSON.stringify(plan));
};
