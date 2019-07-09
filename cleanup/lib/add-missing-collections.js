let file;

(function() {
  if (0 < ARGUMENTS.length) {
    file = ARGUMENTS[0];
  } else {
    print("usage: add-missing-collections.sh --server.endpoint COORDINATOR MISSING-COLLECTIONS-FILE");
    return;
  }

  try {
    let role = db._version(true).details.role;

    if (role !== "COORDINATOR") {
      print("you need to connect to a coordinator, not a " + role);
      return;
    }
  } catch (e) {
    print("FATAL: " + e);
    return;
  }

  // imports
  const fs = require('fs');
  const _ = require('underscore');

  let missingCollections;

  try {
    missingCollections = JSON.parse(fs.read(file));
  } catch (e) {
    print("FATAL: " + e);
    return;
  }

  try {
    _.each(missingCollections, function(entry) {
      print("adding missing collections for database: " + entry.database);

      db._useDatabase(entry.database);

      let c = entry.missing;
      c.sort(function(lhs, rhs) {
        // graphs must always come first, because it is used as sharding prototype in distributeShardsLike
        if (lhs === '_graphs') {
          return -1;
        }
        if (rhs === '_graphs') {
          return 1;
        }
        return lhs < rhs ? -1 : 1;
      });

      c.forEach(function(name) {
        try {
          let properties = { isSystem: true, waitForSync: false, journalSize: 1024 * 1024, replicationFactor: 2 };
          if (name !== "_graphs") {
            properties.distributeShardsLike = "_graphs";
          }

          db._create(name, properties);
          print("created collection " + name);
        } catch (err) {
          // 1207 = duplicate name. this means somebody else has created the collection in the meantime
          if (err.errorNum !== 1207) {
            print("an error occurred while creating missing collection " + name + ": " + String(err));
          }
        }
      });
    });
  } catch (err) {
    print("an error occurred while creating missing collections: " + String(err));
  } finally {
    db._useDatabase("_system");
  }
}());
