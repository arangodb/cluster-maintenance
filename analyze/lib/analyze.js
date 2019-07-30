let file;

if (0 < ARGUMENTS.length) {
  file = ARGUMENTS[0];
}

;
(function () {
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

  let extractPrimaries = function (info, dump) {
    let primariesAll = {};
    let primaries = {};

    const health = dump.arango.Supervision.Health;

    _.each(health, function (server, key) {
      if (key.substring(0, 4) === 'PRMR') {
        primariesAll[key] = server;

        if (server.Status === 'GOOD') {
          primaries[key] = server;
        }
      }
    });

    info.primaries = primaries;
    info.primariesAll = primariesAll;
  };

  let printPrimaries = function (info) {
    var table = new AsciiTable('Primaries');
    table.setHeading('', 'status');

    _.each(info.primariesAll, function (server, name) {
      table.addRow(name, server.Status);
    });

    print(table.toString());
  };

  let setGlobalShard = function (info, shard) {
    let dbServer = shard.dbServer;
    let isLeader = shard.isLeader;

    if (!info.shardsPrimary[dbServer]) {
      info.shardsPrimary[dbServer] = {
        leaders: [],
        followers: [],
        realLeaders: []
      };
    }

    if (isLeader) {
      info.shardsPrimary[dbServer].leaders.push(shard);

      if (shard.isReadLeader) {
        info.shardsPrimary[dbServer].realLeaders.push(shard);
      }
    } else {
      info.shardsPrimary[dbServer].followers.push(shard);
    }
  };

  let extractDatabases = function (info, dump) {
    let databases = {};

    _.each(dump.arango.Plan.Databases, function (database, name) {
      databases[name] = _.extend({
        collections: [],
        shards: [],
        leaders: [],
        followers: [],
        realLeaders: [],
        isSystem: (name.charAt(0) === '_'),
        data: database
      }, database);
    });

    info.databases = databases;
    info.collections = {};
    info.shardsPrimary = {};
    info.zombies = [];
    info.broken = [];

    let allCollections = dump.arango.Plan.Collections;

    _.each(allCollections, function (collections, dbName) {
      let database = databases[dbName];

      _.each(collections, function (collection, cId) {
        if (collection.name === undefined && collection.id === undefined) {
          info.zombies.push({
            database: dbName,
            cid: cId,
            data: collection
          });
        } else if (collection.name === undefined || collection.id === undefined) {
          info.broken.push({
            database: dbName,
            cid: cId,
            collection: collection,
            data: collection
          });
        } else {
          let full = dbName + "/" + collection.name;
          let coll = {
            name: collection.name,
            fullName: full,
            distributeShardsLike: collection.distributeShardsLike || '',
            numberOfShards: collection.numberOfShards,
            replicationFactor: collection.replicationFactor,
            isSmart: collection.isSmart,
            type: collection.type,
            id: cId
          };

          database.collections.push(coll);
          info.collections[full] = coll;

          coll.shards = [];
          coll.leaders = [];
          coll.followers = [];

          _.each(collection.shards, function (shard, sName) {
            coll.shards.push(shard);

            let s = {
              shard: sName,
              database: dbName,
              collection: collection.name
            };

            if (0 < shard.length) {
              coll.leaders.push(shard[0]);
              setGlobalShard(info,
                _.extend({
                  dbServer: shard[0],
                  isLeader: true,
                  isReadLeader: (coll.distributeShardsLike === '')
                }, s));

              for (let i = 1; i < shard.length; ++i) {
                coll.followers.push(shard[i]);
                setGlobalShard(info,
                  _.extend({
                    dbServer: shard[i],
                    isLeader: false
                  }, s));
              }
            }
          });

          if (coll.distributeShardsLike !== '') {
            coll.realLeaders = [];
          } else {
            coll.realLeaders = coll.leaders;
          }

          database.shards = database.shards.concat(coll.shards);
          database.leaders = database.leaders.concat(coll.leaders);
          database.followers = database.followers.concat(coll.followers);
          database.realLeaders = database.realLeaders.concat(coll.realLeaders);
        }
      });
    });
  };

  const extractCollectionIntegrity = (info, dump) => {
    const planCollections = dump.arango.Plan.Collections;
    const planDBs = dump.arango.Plan.Databases;
    info.noPlanDatabases = [];
    info.noShardCollections = [];
    info.realLeaderMissing = [];
    info.leaderOnDeadServer = [];
    info.followerOnDeadServer = [];
    for (const [db, collections] of Object.entries(planCollections)) {
      if (!planDBs.hasOwnProperty(db)) {
        // This database has Collections but is deleted.
        info.noPlanDatabases.push(db, collections);
        continue;
      }
      for (const [name, col] of Object.entries(collections)) {
        const { shards, distributeShardsLike, isSmart } = col;
        if (!shards || (Object.keys(shards).length === 0 && !isSmart) || shards.constructor !== Object) {
          // We do not have shards
          info.noShardCollections.push({ db, name, col });
          continue;
        }

        if (distributeShardsLike && !collections.hasOwnProperty(distributeShardsLike)) {
          // The prototype is missing
          info.realLeaderMissing.push({ db, name, distributeShardsLike, col });
        }

        for (const [shard, servers] of Object.entries(shards)) {
          for (let i = 0; i < servers.length; ++i) {
            if (!info.primaries.hasOwnProperty(servers[i])) {
              if (i == 0) {
                info.leaderOnDeadServer.push({ db, name, shard, server: servers[i], servers });
              } else {
                info.followerOnDeadServer.push({ db, name, shard, server: servers[i], servers });
              }
            }
          }
        }
      }
    }
  };

  const printGood = (msg) => {
    print('Good: ' + msg);
    print();
  };
  
  const printBad = (msg) => {
    // blink blink blink
    print('Bad: ' + msg);
    print();
  };

  const printCollectionIntegrity = (info) => {
    const {
      noPlanDatabases,
      noShardCollections,
      realLeaderMissing,
      leaderOnDeadServer,
      followerOnDeadServer
    } = info;
    let infected = false;
    if (noPlanDatabases.length > 0) {
      printBad('Your cluster has some leftover collections from deleted databases');
      const table = new AsciiTable('Deleted databases with leftover collections');
      table.setHeading('Database');
      for (const d of noPlanDatabases) {
        table.addRow(d);
      }
      print(table.toString());
      infected = true;
    } else {
      printGood('Your cluster does not have any leftover collections from deleted databases');
    }

    if (noShardCollections.length > 0) {
      printBad('Your cluster has some collections without shards');
      const table = new AsciiTable('Collections without shards');
      table.setHeading('Database', 'CID');
      for (const d of noShardCollections) {
        table.addRow(d.db, d.name);
      }
      print(table.toString());
      infected = true;
    } else {
      printGood('Your cluster does not have any collections without shards');
    }

    if (realLeaderMissing.length > 0) {
      printBad('Your cluster misses some collection(s) used as leaders in distributeShardsLike');
      const table = new AsciiTable('Real leader missing for collection');
      table.setHeading('Database', 'CID', 'LeaderCID');
      for (const d of realLeaderMissing) {
        table.addRow(d.db, d.name, d.distributeShardsLike);
      }
      print(table.toString());
      infected = true;
    } else {
      printGood('Your cluster does not miss any collections used as leaders in distributeShardsLike');
    }

    if (leaderOnDeadServer.length > 0) {
      printBad('Your cluster has leaders placed on failed DBServers');
      const table = new AsciiTable('Leader on failed DBServer');
      table.setHeading('Database', 'CID', 'Shard', 'Failed DBServer', 'All Servers');
      for (const d of leaderOnDeadServer) {
        table.addRow(d.db, d.name, d.shard, d.server, JSON.stringify(d.servers));
      }
      print(table.toString());
      infected = true;
    } else {
      printGood('Your cluster does not have any leaders placed on failed DBServers');
    }

    if (followerOnDeadServer.length > 0) {
      printBad('Your cluster has followers placed on failed DBServers');
      const table = new AsciiTable('Follower on failed DBServer');
      table.setHeading('Database', 'CID', 'Shard', 'Failed DBServer', 'All Servers');
      for (const d of followerOnDeadServer) {
        table.addRow(d.db, d.name, d.shard, d.server, JSON.stringify(d.servers));
      }
      print(table.toString());
      infected = true;
    } else {
      printGood('Your cluster does not have any followers placed on failed DBServers');
    }
    return infected;
  };

  const saveCollectionIntegrity = (info) => {
    const {
      noPlanDatabases,
      noShardCollections,
      realLeaderMissing,
      leaderOnDeadServer,
      followerOnDeadServer
    } = info;
    if (noPlanDatabases.length > 0 ||
      noShardCollections.length > 0 ||
      realLeaderMissing.length > 0 ||
      leaderOnDeadServer.length > 0 ||
      followerOnDeadServer.length > 0) {
      fs.write("collectionIntegrity.json", JSON.stringify({
        noPlanDatabases,
        noShardCollections,
        realLeaderMissing,
        leaderOnDeadServer,
        followerOnDeadServer
      }));
    }
  };

  let printDatabases = function (info) {
    var table = new AsciiTable('Databases');
    table.setHeading('', 'collections', 'shards', 'leaders', 'followers', 'Real-Leaders');

    _.each(_.sortBy(info.databases, x => x.name), function (database, name) {
      table.addRow(database.name, database.collections.length, database.shards.length,
        database.leaders.length, database.followers.length,
        database.realLeaders.length);
    });

    print(table.toString());
    return false;
  };

  let printCollections = function (info) {
    var table = new AsciiTable('collections');
    table.setHeading('', 'CID', 'RF', 'Shards Like', 'Shards', 'Type', 'Smart');

    _.each(_.sortBy(info.collections, x => x.fullName), function (collection, name) {
      table.addRow(collection.fullName, collection.id, collection.replicationFactor,
        collection.distributeShardsLike, collection.numberOfShards,
        collection.type, collection.isSmart);
    });
    print(table.toString());
    return false;
  };

  let printPrimaryShards = function (info) {
    var table = new AsciiTable('Primary Shards');
    table.setHeading('', 'Leaders', 'Followers', 'Real Leaders');

    _.each(info.shardsPrimary, function (shards, dbServer) {
      table.addRow(dbServer, shards.leaders.length, shards.followers.length, shards.realLeaders.length);
    });

    print(table.toString());
    return false;
  };

  let printZombies = function (info) {
    if (0 < info.zombies.length) {
      printBad('Your cluster has some zombies');
      var table = new AsciiTable('Zombies');
      table.setHeading('Database', 'CID');

      _.each(info.zombies, function (zombie) {
        table.addRow(zombie.database, zombie.cid);
      });

      print(table.toString());
      return true;
    } else {
      printGood('Your cluster does not have any zombies');
      return false;
    }
  };

  let saveZombies = function (info) {
    if (info.zombies.length > 0) {
      let output = [];

      _.each(info.zombies, function (zombie) {
        output.push({ database: zombie.database, cid: zombie.cid, data: zombie.data });
      });

      fs.write("zombies.json", JSON.stringify(output));

      print("To remedy the zombies issue please run the following command:");
      print(`./cleanup/remove-zombies.sh <all options you pass to analyze.sh> ${fs.makeAbsolute('zombies.json')}`);
      print();
    }
  };

  let printBroken = function (info) {
    if (0 < info.broken.length) {
      printBad('Your cluster has broken collections');
      var table = new AsciiTable('Broken');
      table.setHeading('Database', 'CID');

      _.each(info.broken, function (zombie) {
        table.addRow(zombie.database, zombie.cid);
      });

      print(table.toString());
      return true;
    } else {
      printGood('Your cluster does not have broken collections');
      return false;
    }
  };

  let extractCurrentDatabasesDeadPrimaries = function (info, dump) {
    let databases = [];

    _.each(dump.arango.Current.Databases, function (database, name) {
      _.each(database, function (primary, pname) {
        if (!info.primaries.hasOwnProperty(pname)) {
          databases.push({
            database: name,
            primary: pname,
            data: primary
          });
        }
      });
    });

    info.databasesDeadPrimaries = databases;
  };

  let printCurrentDatabasesDeadPrimaries = function (info) {
    if (0 < info.databasesDeadPrimaries.length) {
      printBad('Your cluster has dead primaries in Current');
      var table = new AsciiTable('Dead primaries in Current');
      table.setHeading('Database', 'Primary');

      _.each(info.databasesDeadPrimaries, function (zombie) {
        table.addRow(zombie.database, zombie.primary);
      });

      print(table.toString());
      return true;
    } else {
      printGood('Your cluster does not have any dead primaries in Current');
      return false;
    }
  };

  let saveCurrentDatabasesDeadPrimaries = function (info) {
    if (info.databasesDeadPrimaries.length > 0) {
      let output = [];

      _.each(info.databasesDeadPrimaries, function (zombie) {
        output.push({ database: zombie.database, primary: zombie.primary, data: zombie.data });
      });

      fs.write("dead-primaries.json", JSON.stringify(output));
      print("To remedy the dead primaries issue please run the following command:");
      print(`./cleanup/remove-dead-primaries.sh <all options you pass to analyze.sh> ${fs.makeAbsolute('dead-primaries.json')}`);
      print();
    }
  };

  let extractEmptyDatabases = function (info) {
    info.emptyDatabases = [];

    _.each(_.sortBy(info.databases, x => x.name), function (database, name) {
      if (database.collections.length === 0 && database.shards.length === 0) {
        info.emptyDatabases.push(database);
      }
    });
  };

  let printEmptyDatabases = function (info) {
    if (0 < info.emptyDatabases.length) {
      printBad('Your cluster has some skeleton databases (databases without collections)');
      var table = new AsciiTable('Skeletons');
      table.setHeading('Database name');

      _.each(info.emptyDatabases, function (database) {
        table.addRow(database.name);
      });

      print(table.toString());
      return true;
    } else {
      printGood('Your cluster does not have any skeleton databases (databases without collections)');
      return false;
    }
  };

  let saveEmptyDatabases = function (info) {
    if (info.emptyDatabases.length > 0) {
      let output = [];

      _.each(info.emptyDatabases, function (skeleton) {
        output.push({ database: skeleton.name, data: skeleton.data });
      });

      fs.write("skeleton-databases.json", JSON.stringify(output));
      print("To remedy the skeleton databases issue please run the following command:");
      print(`./cleanup/remove-skeleton-databases.sh <all options you pass to analyze.sh> ${fs.makeAbsolute('skeleton-databases.json')}`);
      print();
    }
  };
  
  let extractMissingCollections = function (info) {
    info.missingCollections = [];

    _.each(_.sortBy(info.databases, x => x.name), function (database, name) {
      let system = database.collections.filter(function (c) {
        return c.name[0] === '_';
      }).map(function(c) {
        return c.name;
      });

      let missing = [];
      [ "_apps", "_appbundles", "_aqlfunctions", "_graphs", "_jobs", "_queues" ].forEach(function(name) {
        if (system.indexOf(name) === -1) {
          missing.push(name);
        }
      });

      if (missing.length > 0) {
        info.missingCollections.push({ database: database.name, missing });
      }
    });
  };
  
  let printMissingCollections = function (info) {
    if (info.missingCollections.length > 0) {
      printBad('Your cluster is missing relevant system collections:');
      var table = new AsciiTable('Missing collections');
      table.setHeading('Database', 'Collections');

      _.each(info.missingCollections, function (entry) {
        table.addRow(entry.database, entry.missing.join(", "));
      });

      print(table.toString());
      return true;
    } else {
      printGood('Your cluster is not missing relevant system collections');
      return false;
    }
  };
  
  let saveMissingCollections = function (info) {
    if (info.missingCollections.length > 0) {
      let output = info.missingCollections;

      fs.write("missing-collections.json", JSON.stringify(output));
      print("To remedy the missing collections issue please run the following command AGAINST A COORDINATOR:");
      print(`./cleanup/add-missing-collections.sh <options> ${fs.makeAbsolute('missing-collections.json')}`);
      print();
    }
  };

 let extractOutOfSyncFollowers = (info, dump) => {
   const planCollections = dump.arango.Plan.Collections;
   const currentCollections = dump.arango.Current.Collections;
   const compareFollowers = (plan, current) => {
     // If leaders are not equal we are out of sync.
     if(plan[0] != current[0]) {
       return false;
     }
     if (plan.length === 1) {
       // we have not even requested a follower
       return false;
     }
     for (let i = 1; i < plan.length; ++i) {
       const other = current.indexOf(plan[i]);
       if (other < 1) {
         return false;
       }
     }
     return true;
   };
   info.outOfSyncFollowers = [];
   for (const [db, collections] of Object.entries(planCollections)) {
     if (!currentCollections.hasOwnProperty(db)) {
       // database skeleton or  so, don't care
       continue;
     }
     for (const [name, col] of Object.entries(collections)) {
       const { shards } = col;
       if (!shards || Object.keys(shards).length === 0) {
         continue;
       }
       for (const [shard, servers] of Object.entries(shards)) {
         const current = currentCollections[db][name][shard].servers;
         if (!compareFollowers(servers, current)) {
           info.outOfSyncFollowers.push({
             db, name, shard, servers, current
           });
         }
       }

 
      }
   }
 };
 
  const printOutOfSyncFollowers = (info) => {
    const { outOfSyncFollowers } = info;
    const counters = new Map();
    if (outOfSyncFollowers.length > 0) {
      {
        const table = new AsciiTable('Out of sync followers');
        table.setHeading('Database', 'CID', 'Shard', 'Planned', 'Real');
        for (const oosFollower of outOfSyncFollowers ) {
          table.addRow(oosFollower.db, oosFollower.name, oosFollower.shard, oosFollower.servers, oosFollower.current);
          counters.set(oosFollower.servers[0], (counters.get(oosFollower.servers[0]) || 0) + 1)
        }
        print(table.toString());
      }
      {
        const table = new AsciiTable('Number of non-replicated shards per server');
        table.setHeading('Server', 'Number');
        for (const [server, number] of counters.entries()) {
          table.addRow(server, number);
        }
        print(table.toString());
      }
      return true;
    } else {
      print('Your cluster does not have collections where followers are out of sync');
      return false;
    }
  };

  const info = {};

  // extract info
  extractPrimaries(info, dump);
  extractDatabases(info, dump);
  extractCollectionIntegrity(info, dump);
  extractCurrentDatabasesDeadPrimaries(info, dump);
  extractEmptyDatabases(info);
  extractMissingCollections(info);
  extractOutOfSyncFollowers(info, dump);

  let infected = false;

  // Print funny tables
  infected = printPrimaries(info) || infected;
  print();
  infected = printDatabases(info) || infected;
  print();
  infected = printCollections(info) || infected;
  print();
  infected = printPrimaryShards(info) || infected;
  print();
  
  infected = printZombies(info) || infected;
  infected = printBroken(info) || infected;
  infected = printCollectionIntegrity(info) || infected;
  infected = printCurrentDatabasesDeadPrimaries(info) || infected;
  infected = printEmptyDatabases(info) || infected;
  infected = printMissingCollections(info) || infected;
  infected = printOutOfSyncFollowers(info) || infected;
  
  print();

  if (infected) {
    // Save to files
    saveCollectionIntegrity(info);
    saveZombies(info);
    saveCurrentDatabasesDeadPrimaries(info);
    saveEmptyDatabases(info);
    saveMissingCollections(info);
  } else {
    printGood('Did not detect any issues in your cluster');
  }

}());
