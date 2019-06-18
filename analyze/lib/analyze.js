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

  let extractPrimaries = function(info, dump) {
    let primariesAll = {};
    let primaries = {};

    const health = dump.arango.Supervision.Health;

    _.each(health, function(server, key) {
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

  let printPrimaries = function(info) {
    var table = new AsciiTable('Primaries');
    table.setHeading('', 'status');

    _.each(info.primariesAll, function(server, name) {
      table.addRow(name, server.Status);
    });

    print(table.toString());
  };

  let setGlobalShard = function(info, shard) {
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

  let extractDatabases = function(info, dump) {
    let databases = {};

    _.each(dump.arango.Plan.Databases, function(database, name) {
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

    _.each(allCollections, function(collections, dbName) {
      let database = databases[dbName];

      _.each(collections, function(collection, cId) {
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
            type: collection.type
          };

          database.collections.push(coll);
          info.collections[full] = coll;

          coll.shards = [];
          coll.leaders = [];
          coll.followers = [];

          _.each(collection.shards, function(shard, sName) {
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

  let printDatabases = function(info) {
    var table = new AsciiTable('Databases');
    table.setHeading('', 'collections', 'shards', 'leaders', 'followers', 'Real-Leaders');

    _.each(_.sortBy(info.databases, x => x.name), function(database, name) {
      table.addRow(database.name, database.collections.length, database.shards.length,
        database.leaders.length, database.followers.length,
        database.realLeaders.length);
    });

    print(table.toString());
  };

  let printCollections = function(info) {
    var table = new AsciiTable('collections');
    table.setHeading('', 'RF', 'Shards Like', 'Shards', 'Type', 'Smart');

    _.each(_.sortBy(info.collections, x => x.fullName), function(collection, name) {
      table.addRow(collection.fullName, collection.replicationFactor,
        collection.distributeShardsLike, collection.numberOfShards,
        collection.type, collection.isSmart);
    });

    print(table.toString());
  };

  let printPrimaryShards = function(info) {
    var table = new AsciiTable('Primary Shards');
    table.setHeading('', 'Leaders', 'Followers', 'Real Leaders');

    _.each(info.shardsPrimary, function(shards, dbServer) {
      table.addRow(dbServer, shards.leaders.length, shards.followers.length, shards.realLeaders.length);
    });

    print(table.toString());
  };

  let printZombies = function(info) {
    if (0 < info.zombies.length) {
      var table = new AsciiTable('Zombies');
      table.setHeading('Database', 'CID');

      _.each(info.zombies, function(zombie) {
        table.addRow(zombie.database, zombie.cid);
      });
      
      print(table.toString());
    }
  };

  let saveZombies = function(info) {
    let output = [];

    _.each(info.zombies, function(zombie) {
      output.push({ database: zombie.database, cid: zombie.cid, data: zombie.data });
    });
      
    fs.write("zombies.json", JSON.stringify(output));
  };

  let printBroken = function(info) {
    if (0 < info.broken.length) {
      var table = new AsciiTable('Broken');
      table.setHeading('Database', 'CID');

      _.each(info.broken, function(zombie) {
        table.addRow(zombie.database, zombie.cid);
      });
      
      print(table.toString());
    }
  };

  const info = {};

  extractPrimaries(info, dump);
  printPrimaries(info);
  print();

  extractDatabases(info, dump);
  printDatabases(info);
  print();
  printCollections(info);
  print();
  printPrimaryShards(info);
  print();
  printZombies(info);
  saveZombies(info);
  print();
  printBroken(info);
  print();
}());
