/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "trash-dbserver";
exports.group = "cleanup tasks";
exports.args = [
  {
    name: "dbserver",
    optional: false,
    type: "string",
    description: "id of the DBserver"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Moves a DBserver to the trash bin.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.10.99";
exports.info = `
Moves a DBserver to the trash bin.
`;

exports.run = function (extra, args) {

  // imports
  const _ = require("lodash");
  const helper = require('../helper.js');

  // get an agency dump
  const dump = helper.getAgencyDumpFromObjectOrAgency()[0];

  // find server
  const dbserver = helper.getValue("dbserver", args);
  const { serverId, shortName } = helper.findServer(dump, dbserver);

  if (!serverId) {
    helper.fatal(`cannot find server ${dbserver}`);
  }

  print(`INFO: ${serverId} / ${shortName}`);
  const trx = [];

  {
    const oper = {};
    const prec = {};

    const planDBServers = dump.arango.Plan.DBServers;
    const newPlanDBServers = _.extend({}, planDBServers);
    delete newPlanDBServers[serverId];

    const path = "arango/Plan/DBServers";
    oper[path] = newPlanDBServers;
    prec[path] = {old: planDBServers};

    trx.push([oper, prec]);
  }

  {
    const oper = {};
    const prec = {};

    const serversRegistered = dump.arango.Current.ServersRegistered;
    const newServersRegistered = _.extend({}, serversRegistered);
    delete newServersRegistered[serverId];

    const path = "arango/Current/ServersRegistered";
    oper[path] = newServersRegistered;
    prec[path] = {old: serversRegistered};

    trx.push([oper, prec]);
  }

  {
    const oper = {};
    const prec = {};

    const serversKnown = dump.arango.Current.ServersKnown;
    const newServersKnown = _.extend({}, serversKnown);
    delete newServersKnown[serverId];

    const path = "arango/Current/ServersKnown";
    oper[path] = newServersKnown;
    prec[path] = {old: serversKnown};

    trx.push([oper, prec]);
  }

  {
    const oper = {};
    const prec = {};

    const health = dump.arango.Supervision.Health;
    const newHealth = _.extend({}, health);
    delete newHealth[serverId];

    const path = "arango/Supervision/Health";
    oper[path] = newHealth;
    prec[path] = {old: health};

    trx.push([oper, prec]);
  }

  {
    const oper = {};
    const prec = {};

    const short = dump.arango.Target.MapUniqueToShortID;
    const newShort = _.extend({}, short);
    delete newShort[serverId];

    const path = "arango/Target/MapUniqueToShortID";
    oper[path] = newShort;
    prec[path] = {old: short};

    trx.push([oper, prec]);
  }

  {
    const oper = {};

    oper["arango/Plan/Version"] = {op: "increment"};

    trx.push([oper, {}]);
  }

  const res = helper.httpWrapper('POST', '/_api/agency/write', trx);
  print("INFO: " + JSON.stringify(res));

  if (_.indexOf(res.results, 0) >= 0) {
    print("WARNING: pre-condition failed, maybe cleanup already done");
  }
};
