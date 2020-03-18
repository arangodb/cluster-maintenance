/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango */
exports.name = "users-permissions";
exports.group= "standalone tasks";
exports.args = [ 
  { "name" : "mode", "optional" : true, "type" : "string", "description" : "output mode (user = by user, db = by database)" } 
];
exports.args_arangosh = "| --server.endpoint SINGLESERVER-OR-COORDINATOR";
exports.description = "Extracts all users and permissions from the system database.";
exports.selfTests = ["arango", "db"];
exports.requires = "3.3.22 - 3.6.99";
exports.info = `
Extracts all available users and permissions from the _system database
and prints the information.
`;

exports.run = function(extra, args) {
  // imports
  const fs = require('fs');
  const helper = require('../helper.js');
  const users = require("@arangodb/users");
  const AsciiTable = require('../3rdParty/ascii-table');
  const outputType = helper.getValue("mode", args) || 'user';

  let table = new AsciiTable('Permissions');
  try {
    let allUsers = users.all();
    let values = [];
    allUsers.forEach(function(user) {
      let allPermissions = users.permissionFull(user.user);
      let p = Object.keys(allPermissions);
      p.forEach(function(dbName) {
        let perm = allPermissions[dbName].permission;
        if (perm === "undefined") {
          perm = "(inherited)";
        }
        if (outputType === 'user') {
          values.push([user.user, user.active ? "active" : "inactive", dbName, "", perm]);
        } else {
          values.push([dbName, user.user, user.active ? "active" : "inactive", "", perm]);
        }

        let collections = allPermissions[dbName].collections;
        if (collections !== undefined) {
          Object.keys(collections).forEach(function(collectionName) {
            let perm = collections[collectionName];
            if (perm === "undefined") {
              perm = "(inherited)";
            }
            if (outputType === 'user') {
              values.push([user.user, user.active ? "active" : "inactive", dbName, collectionName, perm]);
            } else {
              values.push([dbName, user.user, user.active ? "active" : "inactive", collectionName, perm]);
            }
          });
        }
      });
    });

    if (outputType === 'user') {
      values.sort(function(l, r) {
        if (l[0] !== r[0]) {
          if (l[0] === 'root') {
            return -1;
          } else if (r[0] === 'root') {
            return 1;
          }
          return (l[0] < r[0]) ? -1 : 1;
        }
        if (l[2] !== r[2]) {
          if (l[2] === '*') {
            return -1;
          } else if (r[2] === '*') {
            return 1;
          }
          if (l[2] === '_system') {
            return -1;
          } else if (r[2] === '_system') {
            return 1;
          }
          return (l[2] < r[2]) ? -1 : 1;
        }
        if (l[3] !== r[3]) {
          return (l[3] < r[3]) ? -1 : 1;
        }
        return 0;
      });
      table.setHeading('user', 'active', 'database', 'collection', 'permissions');
    } else if (outputType === 'db') {
      values.sort(function(l, r) {
        if (l[0] !== r[0]) {
          if (l[0] === '*') {
            return -1;
          } else if (r[0] === '*') {
            return 1;
          }
          if (l[0] === '_system') {
            return -1;
          } else if (r[0] === '_system') {
            return 1;
          }
          return (l[0] < r[0]) ? -1 : 1;
        }
        if (l[1] !== r[1]) {
          if (l[1] === 'root') {
            return -1;
          } else if (r[1] === 'root') {
            return 1;
          }
          return (l[1] < r[1]) ? -1 : 1;
        }
        if (l[3] !== r[3]) {
          return (l[3] < r[3]) ? -1 : 1;
        }
        return 0;
      });
      table.setHeading('database', 'user', 'active', 'database', 'collection', 'permissions');
    } else {
      throw "unknown mode '" + outputType + "'. expecting 'user' or 'db'"; 
    }

    values.forEach(function(row) {
      table.addRow(row);
    });
    print(table.toString());
  } catch (ex) {
    helper.fatal(ex)
  }
};
