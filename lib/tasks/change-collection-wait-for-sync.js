/* jshint globalstrict:false, strict:false, sub: true */
/* global print, db */
exports.name = "change-collection-wait-for-sync";
exports.group = "inspect tasks";
exports.args = [
  {
    name: "database",
    optional: false,
    type: "string",
    description: "a single database, a list of databases seperated by comma or '*'"
  },
  {
    name: "waitForSync",
    optional: false,
    type: "boolean",
    description: "new value for 'waitForSync'"
  },
  {
    name: "include-system",
    optional: true,
    type: "boolean",
    description: "include system collections as well; default is false"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Changes the waitForSync in all collections";
exports.selfTests = ["arango", "db", "coordinatorConnection"];
exports.requires = "3.3.23 - 3.9.99";
exports.info = `
Shows properties of all collections.
`;

exports.run = function (extra, args) {

  // imports
  const helper = require('../helper.js');
  const AsciiTable = require('../3rdParty/ascii-table');

  // at what level shall we disply the information
  let dbs = helper.getValue("database", args) || '*';

  db._useDatabase("_system");

  if (dbs === "*") {
    dbs = db._databases();
  } else {
    dbs = dbs.split(",");
  }

  const waitForSync = helper.getValue("waitForSync", args);
  const includeSystem = helper.getValue("include-system", args) || false;
  let change = false;

  // create table of properties
  const table1 = new AsciiTable('Collection Properties');
  const header = ['database', 'collection', 'Wait-For-Sync'];
  table1.setHeading(header);

  for (const dbname of dbs) {
    try {
      db._useDatabase(dbname);

      const cols = db._collections();

      for (const col of cols) {
        const name = col.name();
        const props = col.properties();

        if (includeSystem || name[0] !== '_') {
          if (waitForSync !== props.waitForSync) {
            try {
              col.properties({waitForSync});
              const prop2 = col.properties();
              change = true;

              table1.addRow(dbname, col.name(), props.waitForSync + " => " + prop2.waitForSync);
            } catch (e) {
              helper.printBad("ERROR cannot change collection '" + dbname + "/" + name + "': " + e);
            }
          }
        }
      }
    } catch (e) {
      helper.printBad("ERROR cannot access database '" + dbname + "': " + e);
    } finally {
      db._useDatabase("_system");
    }
  }

  print();
  if (change) {
    print(table1.toString());
  } else {
    helper.printGood("INFO nothing changed");
  }
  print();
};
