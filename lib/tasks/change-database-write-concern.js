/* jshint globalstrict:false, strict:false, sub: true */
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
    description: "database name for which a property is to be changed"
  },
  {
    name: "write-concern",
    optional: false,
    type: "string",
    description: "new write concern value"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Changes write concern of a database";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.6.0 - 3.7.99";
exports.info = `
Changes write concern value of a database property.
`;

exports.run = function (extra, args) {

  const _ = require('lodash');
  const helper = require('../helper.js');
  const parsedFile = helper.getValue("agency-dump", args);
  var agency = helper.getAgencyDumpFromObjectOrAgency(parsedFile);
  const database = helper.getValue("database", args);
  var writeConcern = helper.getValue("write-concern", args);

  if (agency.length != 1) {
    print ("ERROR: invalid agency dump");
    return;
  }
  agency = agency[0];
  if (!agency.hasOwnProperty("arango")) {
    print ("ERROR: invalid agency dump");
    return;
  }
  
  const db = agency.arango.Plan.Databases[database];
  if (db === undefined) {
    print("ERROR: database " + database + " does not exist in this agency");
    return;
  }
  if(writeConcern > db.replicationFactor) {
    print("ERROR: writeConcern higher than the database replication factor");
    return;
  }
  if(writeConcern > Object.keys(agency.arango.Plan.DBServers).length) {
    print("ERROR: writeConcern higher than #database servers");
    return;
  }    
  const cols = agency.arango.Plan.Collections[database];
  
  try {
    writeConcern = parseInt(writeConcern);
  } catch(e) {
    print ("ERROR: cannot convert write-concern parameter to an integer " + e);
    return;
  }

  var trx = [{},{}];
  trx[0]["arango/Plan/Databases/" + database + "/writeConcern"] = writeConcern;
  Object.keys(cols).forEach(
    function (col) {
      trx[0]["arango/Plan/Collections/" + database + "/" + col + "/writeConcern"] = writeConcern
      trx[0]["arango/Plan/Collections/" + database + "/" + col + "/minReplicationFactor"] = writeConcern
      trx[0]["arango/Plan/Version"] = { op : "increment" }
    });
  trx[1]["arango/Plan/Databases/" + database + "/writeConcern"] = db.writeConcern;
  
  let res = helper.httpWrapper('POST', '/_api/agency/write', [trx]);
  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe adjustment has been already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }

};
