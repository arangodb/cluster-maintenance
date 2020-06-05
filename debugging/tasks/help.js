/* jshint globalstrict:false, strict:false, sub: true */
/* global print */
exports.name = "help";
exports.group = "standalone tasks";
exports.args = [ { "name" : "task-name", "optional" : true, "type": "string"} ];
exports.description = "Shows this help.";
exports.requires = "3.3.23 - 4.99.99";
exports.selfTests = [];
exports.info = `
Provides usage information and shows availabe tasks.
`;

exports.run = function(extra, args) {
  const helper = require('../helper.js');
  const taskName = helper.getValue("task-name", args);
  const tasks = extra.tasks;
  const names = Object.keys(tasks);

  if(taskName === undefined || taskName ==="help" || names.indexOf(taskName) < 0) {
    // help usage
    const maxNameLength = names.reduce(function(maxNameLength, name) {
      if (name.length > maxNameLength) {
        return name.length;
      }
      return maxNameLength;
    }, 0);

    const maxVersionLength = Object.values(tasks).map(function(task) {
      return task.requires;
    }).reduce(function(maxVersionLength, version) {
      if (version.length > maxVersionLength) {
        return version.length;
      }
      return maxVersionLength;
    }, 0);

    print();
    print("General Usage: " + global.__filename + " <taskname> [parameters]");
    print("   Help Usage: " + global.__filename + " help <taskname>");
    let lastGroup;
    Object.keys(tasks).forEach(function(key) {
      let currentGroup = tasks[key].group;
      if (currentGroup !== lastGroup) {
        lastGroup = currentGroup;
        print();
        print(currentGroup + ":");
      }
      print("  " + helper.padRight(key, maxNameLength) +
            "    " + helper.padRight(tasks[key].requires, maxVersionLength) +
            "    " + tasks[key].description);
    });
  } else {
    // task specific usage
    print("Usage for task: " + taskName + "\n");
    helper.printUsage(tasks[taskName]);
  }
  print();
};
