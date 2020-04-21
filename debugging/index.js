#!/usr/bin/arangosh --javascript.execute
/* jshint globalstrict:false, strict:false, sub: true */
/* global ARGUMENTS, print, arango, db */

(function () {
  const fs = require("fs");
  const internal = require("internal");
  const semver = require("semver");
  const helper = require(fs.join(__dirname, "helper"));

  const minimumShellVersion = "3.3.0";

  // remove '-*' like '-devel' from version strings
  let [shellVersion, isDevel] = internal.version.split('-');

  isDevel = (isDevel === "devel");

  const fatal = helper.fatal;

  const isEmpty = (obj) => {
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        return false;
      }
    }
    return true;
  };

  const startupSelfTest = function () {
    if (ARGUMENTS.length < 1) {
      fatal("No arguments specified. Need at least the name of the task to execute! Please use `help` to get an overview of all available tasks.");
    }
    if (shellVersion === undefined) {
      fatal("Unable to determine arangosh version. This should not happen. Please try invoking this script via the arangosh!");
    }

    if (!semver.gtr(shellVersion, minimumShellVersion)) {
      fatal("Current arangosh version " + shellVersion + " is not supported. Required at least: " + minimumShellVersion);
    }
  };

  const selfTests = {
    arango: function (args) {
      if (arango === undefined) {
        fatal("arango object is undefined. This should not happen. " +
              "Please try invoking this script via the arangosh!");
      }
    },
    db: function (args) {
      if (db === undefined) {
        fatal("db object is undefined. This should not happen. " +
              "Please try invoking this script via the arangosh!");
      }
    },
    agencyConnection: function (args, options) {
      if (options.findLeader) {
        helper.switchToAgencyLeader();
      }

      helper.checkLeader();
    },
    leaderAgencyConnection: function (args, options) {
      helper.switchToAgencyLeader();
      helper.checkLeader();
    },
    coordinatorConnection: function (args) {
      helper.checkCoordinator();
    }
  };

  const validateTask = function (file, task) {
    let name = task.name;
    if (typeof name !== "string" || name.length === 0) {
      fatal("Task definition from file '" + file + "' does not contain a valid name");
    }
    if (typeof task.group !== "string") {
      fatal("Task definition from file '" + file + "' does not contain a valid group");
    }
    if (typeof task.requires !== "string") {
      fatal("Task definition from file '" + file + "' does not contain a valid requires value");
    }
    if (typeof task.run !== "function") {
      fatal("Task definition from file '" + file + "' does not contain a valid run callback");
    }
    if (typeof task.description !== "string") {
      fatal("Task definition from file '" + file + "' does not contain a valid description");
    }
    if (task.selfTests && !Array.isArray(task.selfTests)) {
      fatal("Task definition from file '" + file + "' does not contain a valid selfTests definition");
    }
    task.selfTests = task.selfTests.map(function (test) {
      if (typeof test === "string") {
        test = { name: test, args: [] };
      }
      if (!selfTests.hasOwnProperty(test.name)) {
        fatal("Task definition from file '" + file + "' contains unknown selfTest '" + test.name + "'");
      }
      return test;
    });
  };

  const compareTasks = (lhs, rhs) => {
    if (lhs.group === rhs.group) {
      return lhs.name < rhs.name ? -1 : 1;
    }
    return lhs.group < rhs.group ? -1 : 1;
  };

  const loadTasks = function (pattern) {
    let tasks = fs.listTree(fs.join(__dirname, "tasks")).filter(function (task) {
      return task.match(/\.js$/);
    }).filter(function (name) {
      return !name.match(/-disabled/);
    }).map(function (task) {
      let file = fs.join(__dirname, "tasks", task);
      try {
        let t = require(file);
        if (!Array.isArray(t.selfTests)) {
          t.selfTests = [];
        }
        validateTask(file, t);
        return t;
      } catch (err) {
        fatal("Unable to load task definition from file '" + file + "': " + String(err));
      }
    }).filter(function (task) {
      let good = semver.satisfies(shellVersion, task.requires) || isDevel;
      if (!good) {
        print("- " + task.name + ": removing this task because of mismatching shell version (" + shellVersion + ") requires: " + task.requires);
      }
      return good;
    });
    tasks.sort(function (lhs, rhs) {
      return compareTasks(lhs, rhs);
    });
    let result = {};
    tasks.forEach(function (task) {
      result[task.name] = task;
    });
    return result;
  };

  startupSelfTest();
  const requestedTask = ARGUMENTS[0];
  const tasks = loadTasks();

  if (isEmpty(tasks)) {
    fatal("No tasks loaded - check version requirements!");
  }

  if (!tasks.hasOwnProperty(requestedTask)) {
    fatal("Requested task '" + requestedTask +
          "' not found. Available tasks: " +
          Object.keys(tasks).join(", "));
  }

  let task = tasks[requestedTask];
  let options = helper.checkGlobalArgs(ARGUMENTS);

  if (task && task.selfTests) {
    task.selfTests.forEach(function (test) {
      selfTests[test.name](test.args || {}, options);
    });
  }

  let args = helper.checkArgs(task, ARGUMENTS);
  task.run({ tasks }, args);
})();
