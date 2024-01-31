/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "show-supervision";
exports.group = "analyze tasks";
exports.args = [];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Shows the state of the supervision.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.12.99";
exports.info = `
  Shows the state of the supervision, maintenance and hot-backup.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');

  // get an agency dump
  const dump = helper.getAgencyDumpFromObjectOrAgency(undefined)[0];
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
  } else {
    print("INFO: /arango/Supervision/Maintenance is not set");
  }

  if (create) {
    print("WARNING: /arango/Target/HotBackup/Create is set to " + JSON.stringify(create));
  } else {
    print("INFO: /arango/Target/HotBackup/Create is not set");
  }
};
