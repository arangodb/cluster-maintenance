let file;

(function() {
  if (0 < ARGUMENTS.length) {
    file = ARGUMENTS[0];
  } else {
    print("usage: remove-skeleton-databases.sh --server.endpoint LEADER-AGENT SKELETON-FILE");
    return;
  }

  if (db === undefined) {
    print("FATAL: database object 'db' not found. Please make sure this script is executed against the leader agent.");
    return;
  }

  try {
    // try to find out the hard way if we are a 3.3 or 3.4 client
    let stringify = false;
    try {
      let ArangoError = require('@arangodb').ArangoError;
      try {
        arango.POST('/_api/agency/read', [["/"]]);
      } catch (err) {
        if (err instanceof ArangoError && err.errorNum === 10) {
          // bad parameter - wrong syntax
          stringify = true;
        }
      }
    } catch (err) {
    }

    let role = db._version(true).details.role;

    if (role === undefined) {
      // potentially ArangoDB 3.3
      print("WARNING: unable to determine server role. You can ignore this warning if the script is executed against the leader agent.");
      role = "AGENT";
    }
    
    if (role === "AGENT") {
      let payload = [["/"]];
      if (stringify) {
        payload = JSON.stringify(payload);
      }
      let agency = arango.POST('/_api/agency/read', payload);

      if (agency.code === 307) {
        print("you need to connect to the leader agent. this agent is a follower.");
        return;
      }
    } else {
      print("you need to connect to the leader agent, not a " + role);
      return;
    }
  } catch (e) {
    print("FATAL: " + e);
    return;
  }

  // imports
  const fs = require('fs');
  const _ = require('underscore');

  let skeletons;

  try {
    skeletons = JSON.parse(fs.read(file));
  } catch (e) {
    print("FATAL: " + e);
    return;
  }

  _.each(skeletons, function(skeleton) {
    if (skeleton.database.length > 0) {
      print("removing skeleton database: " + skeleton.database);

      data = {};
      data['/arango/Plan/Databases/' + skeleton.database] = {
        'op': 'delete'
      };

      pre = {};
      pre['/arango/Plan/Collections/' + skeleton.database] = {
        'oldEmpty': true
      };
      
      pre['/arango/Plan/Databases/' + skeleton.database] = {
        'old': skeleton.data
      };
      
      print(JSON.stringify([[data, pre]]));

      let res = arango.POST('/_api/agency/write', JSON.stringify([[data, pre]]));

      if (res.results[0] === 0) {
        print("WARNING: pre-condition failed, maybe cleanup already done");
      } else {
        print("INFO: " + JSON.stringify(res));
      }
    } else {
      print("ERROR: corrupted entry in skeleton file: " + JSON.stringify(skeleton));
    }
  });
}());
