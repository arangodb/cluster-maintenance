let file;

if (0 < ARGUMENTS.length) {
  file = ARGUMENTS[0];
}

;
(function() {
  // imports
  const fs = require('fs');
  const _ = require('underscore');
  const AsciiTable = require('./ascii-table');

  let dump;

  if (file) {
    print("Using dump file '" + file + "'");

    dump = JSON.parse(fs.read(file));

    if (Array.isArray(dump)) {
      dump = dump[0];
    } else {
      dump = dump.agency;
    }
  } else {
    try {
      let role = db._version(true).details.role;

      if (role === "AGENT") {
        let agency = arango.POST('/_api/agency/read', [
          ["/"]
        ]);

        if (agency.code === 307) {
          print("you need to connect to the leader agent");
          return;
        }

        dump = agency[0];
      } else {
        print("you need to connect to the leader agent, not a " + role);
        return;
      }
    } catch (e) {
      print("FATAL: " + e);
      return;
    }
  }

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
    AsciiTable('Pending');
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
}());
