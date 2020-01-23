/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango */
exports.name = "show-move-shards";
exports.group = "move shard tasks";
exports.args = [ 
  { "name" : "agency-dump", "optional" : true, "type": "jsonfile", "description": "agency dump" } 
];
exports.args_arangosh = "| --server.endpoint LEADER-AGENT";
exports.description = "Allows to inspect shards being moved.";
exports.selfTests = ["arango", "db"];
exports.requires = "3.3.23 - 3.5.99";
exports.info = `
Allows to track progress of shard movement.
`;

exports.run = function(extra, args) {
  // modules
  const helper = require('../helper.js');
  const fs = require('fs');
  const _ = require('underscore');
  const AsciiTable = require('../3rdParty/ascii-table');

  // variables
  const printGood = helper.printGood;
  const parsedFile = helper.getValue("agency-dump", args);
  let dump = helper.getAgencyDumpFromObjectOrAgency(parsedFile)[0];

  let extractTodos = function(info, dump) {
    const target = dump.arango.Target;

    let joblist = {};
    let reasons = {};
    let jobsPerDatabase = {};
    let serverToServerPending = {};
    let serverToServerToDo = {};

    _.each(target, function(jobs, key) {
      if (key === "ToDo" || key === "Pending" || key === "Finished" || key === "Failed") {
        let list = [];

        _.each(jobs, function(job, jkey) {
          if (job.type === "moveShard") {
            list.push(job);

            if (key === "Failed") {
              let reason = job.reason;

              if (!reasons.hasOwnProperty(reason)) {
                reasons[reason] = 0;
              }

              reasons[reason]++;
            } else {
              let database = job.database;

              if (!jobsPerDatabase.hasOwnProperty(database)) {
                jobsPerDatabase[database] = {};
              }

              if (!jobsPerDatabase[database].hasOwnProperty(key)) {
                jobsPerDatabase[database][key] = [];
              }

              jobsPerDatabase[database][key].push(job);
            }

            if (key === "Pending") {
              let to = job.toServer;
              let from = job.fromServer;

              if (!serverToServerPending.hasOwnProperty(to)) {
                serverToServerPending[to] = {};
              }

              if (!serverToServerPending[to].hasOwnProperty(from)) {
                serverToServerPending[to][from] = [];
              }

              serverToServerPending[to][from].push(job);
            }

            if (key === "ToDo") {
              let to = job.toServer;
              let from = job.fromServer;

              if (!serverToServerToDo.hasOwnProperty(to)) {
                serverToServerToDo[to] = {};
              }

              if (!serverToServerToDo[to].hasOwnProperty(from)) {
                serverToServerToDo[to][from] = [];
              }

              serverToServerToDo[to][from].push(job);
            }
          }
        });

        joblist[key] = list;
      }
    });

    info.serverToServerPending = serverToServerPending;
    info.serverToServerToDo = serverToServerToDo;
    info.jobsPerDatabase = jobsPerDatabase;
    info.jobs = joblist;
    info.reasons = reasons;
  };

  let printDatabaseTodos = function(info) {
    if (0 < Object.keys(info.jobsPerDatabase).length) {
      var table = new AsciiTable('Jobs Per Database');
      table.setHeading('database', 'type', 'count');

      _.each(info.jobsPerDatabase, function(database, dname) {
        table.addRow(dname);

        _.each(database, function(jobs, name) {
          table.addRow('', name, jobs.length);
        });
      });

      print(table.toString());
    }
  };

  let printTodos = function(info) {
    var table = new AsciiTable('Jobs');
    table.setHeading('type', 'count');

    _.each(info.jobs, function(jobs, name) {
      table.addRow(name, jobs.length);
    });

    print(table.toString());
  };

  let printFailedReasons = function(info) {
    if (0 < Object.keys(info.reasons).length) {
      var table = new AsciiTable('Reasons for failure');
      table.setHeading('reason', 'count');

      _.each(info.reasons, function(count, name) {
        table.addRow(name, count);
      });

      print(table.toString());
    }
  };

  let printToDoJobs = function(info) {
    var table = new
    AsciiTable('ToDo (planned)');
    table.setHeading('from', 'to', 'count');

    _.each(info.serverToServerToDo, function(froms, to) {
      _.each(froms, function(jobs, from) {
        table.addRow(from, to,
          jobs.length);
      });
    });

    print(table.toString());
  };

  let printPendingJobs = function(info) {
    var table = new
    AsciiTable('Pending');
    table.setHeading('from', 'to', 'count');

    _.each(info.serverToServerPending, function(froms, to) {
      _.each(froms, function(jobs, from) {
        table.addRow(from, to,
          jobs.length);
      });
    });

    print(table.toString());
  };

  const info = {};

  extractTodos(info, dump);
  printDatabaseTodos(info);
  printFailedReasons(info);
  printToDoJobs(info);
  printPendingJobs(info);
  printTodos(info);
};
