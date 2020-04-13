/* jshint globalstrict:false, strict:false, sub: true */
/* global print, db */
exports.name = "create-missing-collections";
exports.group = "cleanup tasks";
exports.args = [
  { "name": "missing-collections-file",
    "optional": false,
    "type": "jsonfile",
    "description": "collections file created by analyze task"
  }
];
exports.args_arangosh = " --server.endpoint COORDINATOR";
exports.description = "Adds missing collections found by the analyze task.";
exports.selfTests = ["arango", "db", "coordinatorConnection"];
exports.requires = "3.3.23 - 3.6.99";
exports.info = `
Adds missing collections found by the analyze task.
`;

exports.run = function (extra, args) {
  // imports
  const _ = require('underscore');
  const helper = require('../helper.js');
  let missingCollections = helper.getValue("missing-collections-file", args);

  try {
    _.each(missingCollections, function (entry) {
      print("adding missing collections for database: " + entry.database);

      db._useDatabase(entry.database);

      let c = entry.missing;
      c.sort(function (lhs, rhs) {
        // graphs must always come first, because it is used as sharding prototype in distributeShardsLike
        if (lhs === '_graphs') {
          return -1;
        }
        if (rhs === '_graphs') {
          return 1;
        }
        return lhs < rhs ? -1 : 1;
      });

      c.forEach(function (name) {
        try {
          let properties = { isSystem: true, waitForSync: false, journalSize: 1024 * 1024, replicationFactor: 2 };
          if (name !== "_graphs") {
            properties.distributeShardsLike = "_graphs";
          }

          db._create(name, properties);
          print("created collection " + name);
        } catch (err) {
          // 1207 = duplicate name. this means somebody else has created the collection in the meantime
          if (err.errorNum !== 1207) {
            print("an error occurred while creating missing collection " + name + ": " + String(err));
          }
        }
      });
    });
  } catch (err) {
    print("an error occurred while creating missing collections: " + String(err));
  } finally {
    db._useDatabase("_system");
  }
};
