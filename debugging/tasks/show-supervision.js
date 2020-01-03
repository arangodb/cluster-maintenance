/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango, db */
exports.name = "show-supervision";
exports.group= "analyze tasks";
exports.args = [];

exports.args_arangosh = " --server.endpoint LEADER-AGENT";
exports.description = "Checks the state of the supervision";
exports.selfTests = ["arango", "db", "agencyConnection"];
exports.requires = "3.3.23 - 3.6.99";
exports.info = `
Checks the state of the supervision.
`;

exports.run = function(extra, args) {
  const helper = require('../helper.js');

  // imports
  const _ = require('underscore');

  // get an agency dump
  const dump = helper.getAgencyDumpFromObjectOrAgency(undefined);
  const state = dump.arango.Supervision.State;
  const maintenance = dump.arango.Supervision.Maintenance;
  const create = dump.arango.Target && dump.arango.Target.HotBackup && dump.arango.Target.HotBackup.Create;

  if (state.Mode === "Normal") {
    if (maintenance || create) {
      print("WARNING: Supervision operating normally, last timestamp " + state.Timestamp);
    } else {
      print("INFO: Supervision operating normally, last timestamp " + state.Timestamp);
    }
  } else {
    print("WARNING: Supervision is in mode " + state.Mode + ", last timestamp " + state.Timestamp);
  }

  if (maintenance) {
    print("WARNING: /arango/Supervision/Maintenance is set to " + JSON.stringify(maintenance));
  }

  if (create) {
    print("WARNING: /arango/Target/HotBackup/Create is set to " + JSON.stringify(create));
  }
};
