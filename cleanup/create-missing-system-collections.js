/* 
 * helper script to create missing system collections 
 * will iterate over the list of databases and check for the availability
 * of the default system collections in them. will create the missing
 * system collections automatically
 *
 * to be used from the arangosh, with a privileged user (i.e. a user that
 * has write privileges for all databases).
 * suitable for use with ArangoDB 3.4. should not be used without adjustment
 * for ArangoDB 3.5 (as it will create too many system collections there -
 * we are not creating all of these system collections in 3.5 by default anymore).
 */
(function() {
  let print = require("internal").print;
  let old = db._name();
  let errors = 0;
  let collections = 0;
  db._useDatabase("_system");

  let dbs = db._databases();
  dbs.sort();
  dbs.forEach(function(name) {
    try {
      db._useDatabase(name);
      print("# checking database " + name);

      [ "_graphs", "_apps", "_appbundles", "_aqlfunctions", "_frontend", "_jobs", "_modules", "_queues", "_routing" ].forEach(function(collection) {
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

        if (collection == "_jobs") {
          c.ensureIndex({ type: "skiplist", fields: ["queue", "status", "delayUntil"], unique: false, sparse: false });
          c.ensureIndex({ type: "skiplist", fields: ["status", "queue", "delayUntil"], unique: false, sparse: false });
        } else if (collection == "_apps") { 
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
})();
