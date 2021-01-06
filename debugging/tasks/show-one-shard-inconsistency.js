/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */
exports.name = "show-one-shard-inconsistency";
exports.group = "analyze tasks";
exports.args = [];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Test if DBServer has not set oneShard database flag for it's databases";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.6.0 - 3.7.99";
exports.info = `
Shows all DBServer and Databases that have not set the oneShard flag correctly.
This servers have issues finding data using aql function 'DOCUMENT' in those databases.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');

  // imports
  const _ = require('lodash');
  const AsciiTable = require('../3rdParty/ascii-table');

  // get an agency dump
  const conf = helper.getAgencyDumpFromObjectOrAgency()[0];
  const health = conf.arango.Supervision.Health;
  const databasesToTest = [];
  let foundInconsitency = false;
  for (const [dbname, {sharding}] of Object.entries(conf.arango.Plan.Databases)) {
    if (sharding === "single") {
      // This database is supposed to be a one shard database
      databasesToTest.push(dbname);
    }
  }
  if (databasesToTest.length > 0) {
    for (const [server, {Status, Endpoint, Host}] of Object.entries(health)) {
      if (server.startsWith("PRMR-") && Status === "GOOD") {
        print("INFO Testing Server '" + server);
        arango.reconnect(Endpoint, "_system");
        for (const vocbase of databasesToTest) {
          db._useDatabase(vocbase);
          const {sharding} = db._properties();
          if (sharding !== "single") {
            foundInconsitency = true;
          }
        }
      }
    }
  }
  
  print();
  print("================================================================================");
  if (foundInconsitency) {
    print("One-Shard issue with DOCUMENT method detected. In some queries DOCUMENT in AQL may return NULL although the document exists.");
  } else {
    print("No One-Shard issue with DOCUMENT method detected.");
  }
  print("================================================================================");
  print();
};
