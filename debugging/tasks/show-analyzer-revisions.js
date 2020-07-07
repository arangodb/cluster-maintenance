/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango */
exports.name = "show-analyzer-revisions";
exports.group = "analyze tasks";
exports.args = [];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Show the list of the analyzer revisions.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.7.0 - 3.7.99";
exports.info = `
Show the list of the analyzer revisions.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');
  const printBad = helper.printBad;

  // imports
  const _ = require('lodash');
  const AsciiTable = require('../3rdParty/ascii-table');

  // get an agency dump
  let res = helper.httpWrapper('POST', '/_api/agency/read', [["arango/Plan/Analyzers"]]);
  
  var table = new AsciiTable('Analyzer Revisions');
  table.setHeading('Database', 'Revision', 'BuildingRevision', 'CoordinatorId', 'RebootId');
  let revisions = res[0].arango.Plan.Analyzers;
  if(revisions !== undefined ) {
    _.each(revisions, function (revision, database) {
      table.addRow(
        database,
        revision.revision,
        revision.buildingRevision,
        revision.hasOwnProperty('coordinator') ? revision.coordinator :'-',
        revision.hasOwnProperty('coordinatorRebootId') ? revision.coordinatorRebootId :'-'
      );
    });
  } else {
    table.addRow(
      'No records',
      '-',
      '-',
      '-',
      '-');
  }
  print();
  print(table.toString());
  print();
};
