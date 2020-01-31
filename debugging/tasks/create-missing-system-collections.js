/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango, db */
exports.name = "create-missing-system-collections";
exports.group= "cleanup tasks";
exports.args = [ ];
exports.args_arangosh = "--server.endpoint COORDINATOR";
exports.description = "Adds missing system collections for all databases (does not require the analyze task).";
exports.selfTests = ["arango", "db", "coordinatorConnection"];
exports.requires = "3.3.23 - 3.6.99";
exports.info = `
Helper script to create missing system collections. It will iterate over the list
of databases and check for the availability of the default system collections
in them. Will create the missing system collections automatically.

To be used from the arangosh, with a privileged user (i.e. a user that has
write privileges for all databases).
`;

exports.run = function(extra, args) {
  const semver = require("semver");
  let old = db._name();
  let errors = 0;
  let collections = 0;

  let colls = [ "_graphs", "_apps", "_appbundles", "_aqlfunctions",
                "_jobs", "_queues"];

  // with 3.5 some collections are obsolete
  const version = db._version();
  if(semver.lt(version, "3.5.0")) {
    colls = colls.concat([ "_modules", "_frontend", "_routing" ]);
  }

  db._useDatabase("_system");
  let dbs = db._databases();
  dbs.sort();
  dbs.forEach(function(name) {
    try {
      db._useDatabase(name);
      print("# checking database " + name);

      colls.forEach(function(collection) {
        let c = db._collection(collection);
        if (c !== null) {
          return;
        }

        let properties = { isSystem: true, waitForSync: false, journalSize: 1024 * 1024, replicationFactor: 2 };
        if (collection !== "_graphs") {
          properties.distributeShardsLike = "_graphs";
        }

        print("- creating collection " + collection + " in db " + name + " with properties " + JSON.stringify(properties));
        c = db._create(collection, properties);
        ++collections;

        if (collection === "_jobs") {
          c.ensureIndex({ type: "skiplist", fields: ["queue", "status", "delayUntil"], unique: false, sparse: false });
          c.ensureIndex({ type: "skiplist", fields: ["status", "queue", "delayUntil"], unique: false, sparse: false });
        } else if (collection === "_apps") {
          c.ensureIndex({ type: "hash", fields: ["mount"], unique: true, sparse: true });
        }
      });
    } catch (err) {
      print("- !!! caught an error when dealing with database " + name + ": " + String(err));
      print();
      ++errors;
    }
  });

  db._useDatabase(old);

  print();
  print("created " + collections + " collection(s), got " + errors + " error(s)");
  print();
};
