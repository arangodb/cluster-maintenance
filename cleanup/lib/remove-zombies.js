let file;

(function() {
  if (0 < ARGUMENTS.length) {
    file = ARGUMENTS[0];
  } else {
    print("usage: remove-zombies.sh --server.endpoint LEADER-AGENT ZOMBIE-FILE");
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
      print("WARNING: unable to determine server role. You can ignore this warning if the script is executed against an agent.");
      role = "AGENT";
    }

    if (role === "AGENT") {
      let payload = [["/"]];
      if (stringify) {
        payload = JSON.stringify(payload);
      }
      let agency = arango.POST('/_api/agency/read', payload);

      if (agency.code === 307) {
        print("you need to connect to the leader agent");
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

  let zombies;

  try {
    zombies = JSON.parse(fs.read(file));
  } catch (e) {
    print("FATAL: " + e);
    return;
  }

  _.each(zombies, function(zombie) {
    if (zombie.database.length > 0 && zombie.cid.length > 0) {
      print("removing zombie collection: " + zombie.database + "/" + zombie.cid);

      data = {};
      data['/arango/Plan/Collections/' + zombie.database + '/' + zombie.cid] = {
        'op': 'delete'
      };

      pre = {};
      pre['/arango/Plan/Collections/' + zombie.database + '/' + zombie.cid] = {
        'old': zombie.data
      };
      
      let res = arango.POST('/_api/agency/write', JSON.stringify([[data, pre]]));

      if (res.results[0] === 0) {
        print("WARNING: pre-condition failed, maybe cleanup already done");
      } else {
        print("INFO: " + JSON.stringify(res));
      }
    } else {
      print("ERROR: corrupted entry in zombie file: " + JSON.stringify(zombie));
    }
  });
}());
