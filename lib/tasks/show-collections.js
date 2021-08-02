/* jshint globalstrict:false, strict:false, sub: true */
/* global print, db */
exports.name = "show-collections";
exports.group = "inspect tasks";
exports.args = [
  {
    name: "database",
    optional: true,
    type: "string",
    description: "a single database, a list of databases seperated by comma or '*'"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Shows information about all collections";
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

  // create table of properties
  const table1 = new AsciiTable('Collection Properties');
  const header = ['database', 'collection', 'Wait-For-Sync', 'Shards',
    'RF', 'WC'];
  table1.setHeading(header);

  for (const dbname of dbs) {
    try {
      db._useDatabase(dbname);

      const cols = db._collections();

      for (const col of cols) {
        const props = col.properties();
        table1.addRow(dbname, col.name(), props.waitForSync, props.numberOfShards,
          props.replicationFactor, props.writeConcern);
      }
    } catch (e) {
      helper.printBad("ERROR cannot access database '" + dbname + "': " + e);
    } finally {
      db._useDatabase("_system");
    }
  }

  print();
  print(table1.toString());
  print();
};
