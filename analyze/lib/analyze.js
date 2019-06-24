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
        isSystem: (name.charAt(0) === '_')
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
      const table = new AsciiTable('Deleted Databases with leftover Collections');
      table.setHeading('Database');
      for (const d of noPlanDatabases) {
        table.addRow(d);
      }
      print(table.toString());
      infected = true;
    } else {
      print('You cluster is not infected by leftover Collections');
    }

    if (noShardCollections.length > 0) {
      const table = new AsciiTable('Collections without shards');
      table.setHeading('Database', 'CID');
      for (const d of noShardCollections) {
        table.addRow(d.db, d.name);
      }
      print(table.toString());
      infected = true;
    } else {
      print('You cluster is not infected by collections without shards');
    }

    if (realLeaderMissing.length > 0) {
      const table = new AsciiTable('Real Leader missing for collection');
      table.setHeading('Database', 'CID', 'LeaderCID');
      for (const d of realLeaderMissing) {
        table.addRow(d.db, d.name, d.distributeShardsLike);
      }
      print(table.toString());
      infected = true;
    } else {
      print('You cluster is not infected by missing distributeShardsLike Leaders');
    }

    if (leaderOnDeadServer.length > 0) {
      const table = new AsciiTable('Leader on Failed DBServer');
      table.setHeading('Database', 'CID', 'Shard', 'Failed DBServer', 'All Servers');
      for (const d of leaderOnDeadServer) {
        table.addRow(d.db, d.name, d.shard, d.server, JSON.stringify(d.servers));
      }
      print(table.toString());
      infected = true;
    } else {
      print('You cluster is not infected by leaders on failed DBServer');
    }

    if (followerOnDeadServer.length > 0) {
      const table = new AsciiTable('Follower on Failed DBServer');
      table.setHeading('Database', 'CID', 'Shard', 'Failed DBServer', 'All Servers');
      for (const d of followerOnDeadServer) {
        table.addRow(d.db, d.name, d.shard, d.server, JSON.stringify(d.servers));
      }
      print(table.toString());
      infected = true;
    } else {
      print('You cluster is not infected by followers on failed DBServer');
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
      var table = new AsciiTable('Zombies');
      table.setHeading('Database', 'CID');

      _.each(info.zombies, function (zombie) {
        table.addRow(zombie.database, zombie.cid);
      });

      print(table.toString());
      return true;
    } else {
      print('You cluster is not infected by Zombies');
      return false
    }
  };

  let saveZombies = function (info) {
    if (info.zombies.length > 0) {
      let output = [];

      _.each(info.zombies, function (zombie) {
        output.push({ database: zombie.database, cid: zombie.cid, data: zombie.data });
      });

      fs.write("zombies.json", JSON.stringify(output));

      print("To remove Zombies infection please run the following command:");
      print(`./cleanup/remove-zombies.sh <all options you pass to analyze.sh> ${fs.makeAbsolute('zombies.json')}`);
    }
  };

  let printBroken = function (info) {
    if (0 < info.broken.length) {
      var table = new AsciiTable('Broken');
      table.setHeading('Database', 'CID');

      _.each(info.broken, function (zombie) {
        table.addRow(zombie.database, zombie.cid);
      });

      print(table.toString());
      return true;
    } else {
      print('You cluster is not infected by Broken collections');
      return false
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
      var table = new AsciiTable('Dead Primaries in Current');
      table.setHeading('Database', 'Primary');

      _.each(info.databasesDeadPrimaries, function (zombie) {
        table.addRow(zombie.database, zombie.primary);
      });

      print(table.toString());
      return true;
    } else {
      print('Your cluster is not infected by Dead Primaries');
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
      print("To remove dead Primaries infection please run the following command:");
      print(`./cleanup/remove-dead-primaries.sh <all options you pass to analyze.sh> ${fs.makeAbsolute('dead-primaries.json')}`);
    }
  };

  const info = {};


  // extract info
  extractPrimaries(info, dump);
  extractDatabases(info, dump);
  extractCollectionIntegrity(info, dump);
  extractCurrentDatabasesDeadPrimaries(info, dump);
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
  print();
  infected = printBroken(info) || infected;
  print();
  infected = printCollectionIntegrity(info) || infected;
  print();
  infected = printCurrentDatabasesDeadPrimaries(info) || infected;
  print();

  if (infected) {
    // Save to files
    saveCollectionIntegrity(info);
    saveZombies(info);
    saveCurrentDatabasesDeadPrimaries(info);
  } else {
    print('Did not detect any infection in your cluster');
  }

}());
