/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango */
exports.name = "show-agents";
exports.group = "analyze tasks";
exports.args = [];
exports.args_arangosh = " --server.endpoint AGENT";
exports.description = "Show the status of the agents.";
exports.selfTests = ["arango", "db"];
exports.requires = "3.3.23 - 3.7.99";
exports.info = `
Check the state of the agents.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');
  const printBad = helper.printBad;

  // imports
  const _ = require('underscore');
  const AsciiTable = require('../3rdParty/ascii-table');

  // get an agency dump
  const conf = helper.getAgencyConfiguration();
  const pool = conf.configuration.pool;
  const active = conf.configuration.active;
  const errors = {};

  var table1 = new AsciiTable('Agents');
  table1.setHeading('ID', 'leader', 'addr', 'pool', 'size');

  var table2 = new AsciiTable('Terms');
  table2.setHeading('ID', 'term', 'commit', 'last compact', 'next compact');

  _.each(active, function (key) {
    let ip = pool[key];

    try {
      arango.reconnect(ip, "_system");
      const local = helper.getAgencyConfiguration();

      table1.addRow(
        local.configuration.id,
        local.leaderId,
        ip,
        local.configuration["pool size"],
        local.configuration["agency size"]);

      table2.addRow(
        local.configuration.id,
        local.term,
        local.commitIndex,
        local.lastCompactionAt,
        local.nextCompactionAfter);

      if (conf.configuration["agency size"] !== local.configuration["agency size"]) {
        errors['SIZE_MISMATCH'] = "agency-size mismatch";
      }

      if (conf.configuration["pool size"] !== local.configuration["pool size"]) {
        errors['POOL_MISMATCH'] = "pool-size mismatch";
      }

      if (key !== local.configuration.id) {
        errors['ID_MISMATCH'] = "agent id mismatch '" +
          key + " != '" + local.configuration.id +
          " at '" + ip + "'";
      }
    } catch (ex) {
      table1.addRow(
        key,
        'failed to connecto to',
        ip,
        '-',
        '-');
    }
  });

  print();
  print(table1.toString());
  print();
  print(table2.toString());
  print();

  _.each(errors, function (msg) {
    printBad(msg);
  });
};
