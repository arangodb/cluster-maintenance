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
exports.requires = "3.3.23 - 3.5.99";
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
  const outputType = helper.getValue("mode", args) || '';

  let table = new AsciiTable('Permissions');
  try {
    let allUsers = users.all();
    let values = [];
    allUsers.forEach(function(user) {
      let allPermissions = users.permission(user.user);
      let p = Object.keys(allPermissions);
      p.forEach(function(dbName) {
        values.push([dbName, user.user, user.active ? "active" : "inactive", allPermissions[dbName]]);
      });
    });

    if (outputType === '' || outputType === 'user') {
      values.sort(function(l, r) {
        if (l[0] !== r[0]) {
          if (l[0] === 'root') {
            return -1;
          } else if (r[0] === 'root') {
            return 1;
          }
          return (l[0] < r[0]) ? -1 : 1;
        }
        if (l[1] !== r[1]) {
          if (l[1] === '_system') {
            return -1;
          } else if (r[1] === '_system') {
            return 1;
          }
          return (l[1] < r[1]) ? -1 : 1;
        }
        return 0;
      });
      table.setHeading('user', 'active', 'database', 'permissions');
    } else if (outputType === 'db') {
      values.sort(function(l, r) {
        if (l[0] !== r[0]) {
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
        return 0;
      });
      table.setHeading('database', 'user', 'active', 'database', 'permissions');
    }

    values.forEach(function(row) {
      table.addRow(row);
    });
    print(table.toString());
  } catch (ex) {
    helper.fatal("cannot get information: " + ex)
  }
};
