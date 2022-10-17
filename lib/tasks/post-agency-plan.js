/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "post-agency-plan";
exports.group = "Post an agency plan to a new leader agency. Only for debug purpose! DO NOT USE IN PRODUCTION!";
exports.args = [
  {
    "name": "dump-file",
    "optional": false,
    "type": "jsonfile",
    "description": "agency dump file"
  }
];
exports.args_arangosh = "| --server.endpoint LEADER-AGENT";
exports.description = "Posts an agency dump to an ArangoDB agency leader.";
exports.selfTests = ["arango", "db"];
exports.requires = "3.3.23 - 3.10.99";
exports.info = `
This task takes an agency dump file, modifies it to fit to the new server and post it.

    ./maintenance.sh post-agency-plan --server.endpoint tcp://(ip-address):(agency-port)> agencyDump.json
`;

exports.run = function (extra, args) {

  // imports
  const helper = require('../helper.js');
  const _ = require('lodash');

  // needs to be connected to the leader agent
  helper.checkLeader();

  // variables
  const file = helper.getValue("dump-file", args);
  const dump = helper.getAgencyDumpFromObjectOrAgency(file)[0];
  const agencyPlan = dump.arango.Plan;
  const health = dump.arango.Supervision.Health;

  let getNamesOfCurrentDatabaseServers = function (role) {
    let names = [];
    _.each(health, function (info, serverName) {
      if (serverName.substring(0, 4) === 'PRMR') {
        // found dbserver
        if (info.Status === 'GOOD') {
          names.push(serverName);
        }
      }
    });
    return names;
  };

  let oldDBServernames = getNamesOfCurrentDatabaseServers('PRMR');
  let x = helper.getAgencyDumpFromObjectOrAgency(undefined)[0];

  let newDBServerNames = [];
  _.each(x.arango.Plan.DBServers, function (content, name) {
    newDBServerNames.push(name);
  });

  let tmp = JSON.stringify(agencyPlan);
  for (let i = 0; i < oldDBServernames.length; i++) {
    var re = new RegExp(oldDBServernames[i], "g");
    tmp = tmp.replace(re, newDBServerNames[i]);
  }
  let newAgencyPlan = JSON.parse(tmp);
  newAgencyPlan.Version = (x.arango.Plan.Version + 1);

  let pre = {};
  let data = {};
  data['/arango/Plan'] = newAgencyPlan;

  print("Trying to post agency plan to agency.");
  let res = helper.httpWrapper('POST', '/_api/agency/write', [[data, pre]]);

  if (res.results[0] === 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  } else {
    print("INFO: " + JSON.stringify(res));
  }
};
