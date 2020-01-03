/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango, db */
exports.name = "clear-maintenance";
exports.group= "cleanup tasks";
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
    print("INFO: Supervision operating normally, last timestamp " + state.Timestamp);
  }

  const data = [[]];
  const obj = {};

  if (maintenance) {
    const k = '/arango/Supervision/Maintenance';
    print("INFO: " + k + " is set to " + JSON.stringify(maintenance));
    print("INFO: clearing " + k);
    obj[k] = { "op": "delete" };
  }

  if (create) {
    const k = '/arango/Target/HotBackup/Create';
    print("INFO: " + k + " is set to " + JSON.stringify(create));
    print("INFO: clearing " + k);
    obj[k] = { "op": "delete" };
  }

  data[0].push(obj);

  if (maintenance || create) {
    const res = helper.httpWrapper('POST','/_api/agency/write', data);

    if (res.results[0] === 0) {
      print("WARNING: pre-condition failed, maybe cleanup already done");
    } else {
      print("INFO: " + JSON.stringify(res));
    }
  } else {
    print("INFO: nothing to do");
  }
};
