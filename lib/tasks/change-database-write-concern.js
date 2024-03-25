/* jshint globalstrict:false, strict:false, sub:true */
/* global print */
exports.name = "change-database-write-concern";
exports.group = "standalone tasks";
exports.args = [
  {
    name: "agency-dump",
    optional: false,
    type: "jsonfile",
    description: "agency dump"
  },
  {
    name: "database",
    optional: false,
    type: "string",
    description: "database name for which a the write concern value is to be changed"
  },
  {
    name: "write-concern",
    optional: false,
    type: "string",
    description: "new write concern value"
  },
  {
    name: "dry-run",
    optional: true,
    type: "string",
    description: "dry run to only review the agency transaction ([true|false] default: false)"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Changes write concern of a database";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.6.0 - 3.12.99";
exports.info = `
Changes write concern value of a database.
`;

exports.run = function (extra, args) {

  const helper = require('../helper.js');
  const parsedFile = helper.getValue("agency-dump", args);
  var agency = helper.getAgencyDumpFromObjectOrAgency(parsedFile);
  const database = helper.getValue("database", args);
  const dryRun = helper.getValue("dry-run", args);
  var writeConcern = helper.getValue("write-concern", args);

  if (agency.length !== 1) {
    print("ERROR: invalid agency dump");
    return;
  }
  agency = agency[0];
  if (!Object.prototype.hasOwnProperty.call(agency, "arango")) {
    print("ERROR: invalid agency dump");
    return;
  }

  const db = agency.arango.Plan.Databases[database];
  if (db === undefined) {
    print("ERROR: database " + database + " does not exist in this agency");
    return;
  }

  if (writeConcern > db.replicationFactor) {
    print("ERROR: writeConcern higher than the database replication factor");
    return;
  }
  if (writeConcern > Object.keys(agency.arango.Plan.DBServers).length) {
    print("ERROR: writeConcern higher than #database servers");
    return;
  }
  const cols = agency.arango.Plan.Collections[database];

  try {
    writeConcern = parseInt(writeConcern);
  } catch (e) {
    print("ERROR: cannot convert write-concern parameter to an integer " + e);
    return;
  }

  var opers = {};
  opers["arango/Plan/Databases/" + database + "/writeConcern"] = writeConcern;
  Object.keys(cols).forEach(
    function (col) {
      opers["arango/Plan/Collections/" + database + "/" + col + "/writeConcern"] = writeConcern; // the database
      opers["arango/Plan/Collections/" + database + "/" + col + "/minReplicationFactor"] = writeConcern; // existing collections
      opers["arango/Plan/Version"] = {op: "increment"}; // Plan/Version increment
    });

  var precs = {};
  precs["arango/Plan/Databases/" + database + "/writeConcern"] = db.writeConcern; // still with known write concern
  precs["arango/Cluster"] = agency.arango.Cluster; // same cluster as dump

  const trx = [[opers, precs]];
  if (dryRun === "true") {
    print(trx);
  } else {
    const res = helper.httpWrapper('POST', '/_api/agency/write', trx);
    if (res.results[0] === 0) {
      print("WARNING: pre-condition failed, maybe adjustment has been already done");
    } else {
      print("INFO: " + JSON.stringify(res));
    }
  }

};
