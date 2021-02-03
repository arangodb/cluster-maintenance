/* jshint globalstrict:false, strict:false, sub: true */
/* global print, db, arango */
exports.name = "analyze";
exports.group = "analyze tasks";
exports.args = [
  {
    name: "agency-dump",
    optional: true,
    type: "jsonfile",
    description: "agency dump"
  }
];
exports.args_arangosh = "| --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Performs health analysis on your cluster and produces input files for other cleanup tasks.";
exports.selfTests = ["arango", "db"];
exports.requires = "3.3.23 - 3.7.99";
exports.info = `
Runs the analyze task against a cluster. It will create files and print
commands to fix some known problems like the removal of zombies or dead
primaries or creation of missing system collections.
`;

exports.run = function (extra, args) {
  // imports
  const fs = require('fs');
  const _ = require('lodash');
  const AsciiTable = require('../3rdParty/ascii-table');
  const helper = require('../helper.js');

  const printGood = helper.printGood;
  const printBad = helper.printBad;

  const parsedFile = helper.getValue("agency-dump", args);
  const response = helper.getAgencyDumpFromObjectOrAgency(parsedFile);
  const dump = response[0];
  const stores = response[1];

  const extractFailed = (info, dump) => {
    const failedInstanceEndpoints = [];
    const health = dump.arango.Supervision.Health;
    _.each(health, function (server, key) {
      if (server.Status === 'FAILED') {
        let endpoint = "";
        if (server.Endpoint.startsWith("ssl")) {
          endpoint = server.Endpoint.replace("ssl:", "https:");
        } else {
          endpoint = server.Endpoint.replace("tcp:", "http:");
        }
        failedInstanceEndpoints.push(endpoint);
      }
    });
    info.failedInstances = failedInstanceEndpoints;
  };

  const saveZombieCallbacks = function (info) {
    const zombieCallbacks = [];
    if (info.failedInstances.length > 0 && info.callbacks !== undefined) {
      Array.prototype.forEach.call(info.callbacks, callback => {
        const url = Object.keys(callback)[0];
        const fs = url.indexOf("/");
        const end = url.indexOf("/", fs + 2);
        if (info.failedInstances.includes(url.slice(0, end))) {
          zombieCallbacks.push(callback);
        }
      });
    }
    if (zombieCallbacks.length > 0) {
      fs.write("zombie-callbacks.json", JSON.stringify(zombieCallbacks));
      print(" To remedy the zombies callback issue please run the task `remove-zombie-callbacks` against the leader AGENT, e.g.:");
      print(` ./maintenance.sh <options> remove-zombie-callbacks ${fs.makeAbsolute('zombie-callbacks.json')}`);
      print();
    }
  };

  const zombieCoordinators = (info, dump) => {
    const plannedCoords = dump.arango.Plan.Coordinators;
    const currentCoords = dump.arango.Current.Coordinators;
    const zombies = [];

    _.each(Object.keys(currentCoords), function (id) {
      if (!_.has(plannedCoords, id)) {
        zombies.push(id);
      }
    });

    info.zombieCoordinators = zombies;
    if (zombies.length > 0) {
      return true;
    } else {
      return false;
    }

  };

  const zombieAnalyzerRevisions = (info, dump) => {
    const plannedRevisions = dump.arango.Plan.Analyzers;
    const plannedDatabases = dump.arango.Plan.Databases;
    const zombies = [];

    if (plannedRevisions !== undefined) {
      _.each(Object.keys(plannedRevisions), function (id) {
        if (!_.has(plannedDatabases, id)) {
          zombies.push(id);
        }
      });
    }

    info.zombieAnalyzerRevisions = zombies;
    if (zombies.length > 0) {
      return true;
    } else {
      return false;
    }

  };

  const printPrimaries = function (info) {
    const table = new AsciiTable('Primaries');
    table.setHeading('', 'status');

    _.each(info.primariesAll, function (server, name) {
      table.addRow(name, server.Status);
    });

    print(table.toString());
    return false;
  };

  const printZombieCoordinators = function (info) {
    const haveZombies = info.zombieCoordinators.length > 0;
    if (!haveZombies) {
      printGood('Your cluster does not have any zombie coordinators');
      return false;
    } else {
      printBad('Your cluster has zombie coordinators');
      return true;
    }
  };

  const printZombieAnalyzerRevisions = function (info) {
    const haveZombies = info.zombieAnalyzerRevisions.length > 0;
    if (!haveZombies) {
      printGood('Your cluster does not have any zombie analyzer revisions');
      return false;
    } else {
      printBad('Your cluster has zombie analyzer revisions');
      return true;
    }
  };

  const printCleanedFailoverCandidates = function (info) {
    const haveCleanedFailovers = (Object.keys(info.correctFailoverCandidates > 0).length);
    if (!haveCleanedFailovers) {
      printGood('Your cluster does not have any cleaned servers for failover');
      return false;
    } else {
      printBad('Your cluster has cleaned servers scheduled for failover');
      return true;
    }
  };

  const recursiveMapPrinter = (map) => {
    if (map instanceof Map) {
      const res = {};
      for (const [k, v] of map) {
        res[k] = recursiveMapPrinter(v);
      }
      return res;
    } else if (map instanceof Array) {
      return map.map(v => recursiveMapPrinter(v));
    } else if (map instanceof Object) {
      const res = {};
      for (const [k, v] of Object.entries(map)) {
        res[k] = recursiveMapPrinter(v);
      }
      return res;
    } else if (map instanceof Set) {
      const res = [];
      for (const v of map.values()) {
        res.push(recursiveMapPrinter(v));
      }
      return res;
    }
    return map;
  };

  const extractCollectionIntegrity = (info, dump) => {
    const planCollections = dump.arango.Plan.Collections;
    const currentCollections = dump.arango.Current.Collections;
    const planDBs = dump.arango.Plan.Databases;
    info.noPlanDatabases = [];
    info.noShardCollections = [];
    info.realLeaderMissing = [];
    info.leaderOnDeadServer = [];
    info.followerOnDeadServer = [];
    for (const [db, collections] of Object.entries(planCollections)) {
      if (!_.has(planDBs, db)) {
        // This database has Collections but is deleted.
        info.noPlanDatabases.push(db, collections);
        continue;
      }
      for (const [name, col] of Object.entries(collections)) {
        const {shards, distributeShardsLike, isSmart} = col;
        if (!shards || (Object.keys(shards).length === 0 && !isSmart) || shards.constructor !== Object) {
          // We do not have shards
          info.noShardCollections.push({db, name, col});
          continue;
        }

        if (distributeShardsLike && !_.has(collections, distributeShardsLike)) {
          // The prototype is missing
          info.realLeaderMissing.push({db, name, distributeShardsLike, col});
        }

        for (const [shard, servers] of Object.entries(shards)) {
          const currentServers = currentCollections[db][name][shard].servers;

          for (let i = 0; i < servers.length; ++i) {
            if (!_.has(info.primaries, servers[i])) {
              if (i === 0) {
                info.leaderOnDeadServer.push({db, name, shard, server: servers[i], servers, currentServers});
              } else {
                info.followerOnDeadServer.push({db, name, shard, server: servers[i], servers, currentServers});
              }
            }
          }

          const diff = _.difference(currentServers, servers);

          for (const entry of diff) {
            if (!_.has(info.primaries, entry)) {
              info.followerOnDeadServer.push({db, name, shard, server: entry, currentServers});
            }
          }
        }
      }
    }
  };

  const extractDistributionGroups = (info, dump) => {
    const planCollections = dump.arango.Plan.Collections;
    const currentCollections = dump.arango.Current.Collections;
    /*
    * realLeaderCid => {
    *   plan => cid => [{shard (sorted), servers: [Leader, F1, F2, F3]}],
    *   current => cid => [{shard (sorted), servers: [Leader, F1, F2, F3]}],
    *   db = dbName
    * }
    */
    const shardGroups = new Map();
    // real leader cid
    const violatedDistShardLike = new Set();
    // {cid, shard, search}
    const noInsyncFollower = new Set();
    // {cid, shard, search}
    const unplannedLeader = new Set();
    // {cid, shard, search}
    const noInsyncAndDeadLeader = new Set();
    for (const [db, collections] of Object.entries(planCollections)) {
      for (const [cid, col] of Object.entries(collections)) {
        const {shards, distributeShardsLike} = col;
        if (!shards || Object.keys(shards).length === 0 || shards.constructor !== Object) {
          // We do not have shards
          continue;
        }
        // If we have DistLike we search for it, otherwise we are leader
        const search = distributeShardsLike || cid;
        const isNewEntry = !shardGroups.has(search);
        if (isNewEntry) {
          shardGroups.set(search, {
            plan: new Map(),
            current: new Map(),
            db
          });
        }
        // Every group is a object of
        // plan => cid => [{shard (sorted), servers: [Leader, F1, F2, F3]}]
        // current => cid => [{shard (sorted), servers: [Leader, F1, F2, F3]}]
        const group = shardGroups.get(search);
        const myPlan = [];
        const myCurrent = [];
        for (const [shard, servers] of Object.entries(shards)) {
          try {
            const curServers = currentCollections[db][cid][shard].servers;
            myPlan.push({shard, servers});
            myCurrent.push({shard, servers: curServers});
            if (curServers[0] !== servers[0]) {
              unplannedLeader.add({cid, shard, search});
            }
            if (servers.length > 1 && curServers.length <= 1) {
              noInsyncFollower.add({cid, shard, search});
              if (!_.has(info.primaries, curServers[0])) {
                noInsyncAndDeadLeader.add({cid, shard, search});
              }
            }
          } catch (e) {}
        }

        myPlan.sort((l, r) => l.shard > r.shard);
        myCurrent.sort((l, r) => l.shard > r.shard);

        if (!isNewEntry) {
          // Pick any of the existing, they need to be all equal, or at least one needs to be reported
          const comp = group.plan.values().next().value;
          for (let i = 0; i < comp.length; ++i) {
            if (comp[i] !== myPlan[i]) {
              // We have at least one mismatch of plans that violate distribution
              violatedDistShardLike.add(search);
              break;
            }
          }
        }
        group.plan.set(cid, myPlan);
        group.current.set(cid, myCurrent);
      }
    }

    info.shardGroups = shardGroups;
    info.violatedDistShardLike = violatedDistShardLike;
    info.noInsyncFollower = noInsyncFollower;
    info.unplannedLeader = unplannedLeader;
    info.noInsyncAndDeadLeader = noInsyncAndDeadLeader;
  };

  const extractInconsistentOneShardDatabases = (info, dump) => {
    info.oneShardIncensistencyDetected = new Set();
    if (info.__usesLiveServer) {
      const health = dump.arango.Supervision.Health;
      const databasesToTest = [];
      for (const [dbname, {sharding}] of Object.entries(dump.arango.Plan.Databases)) {
        if (sharding === "single") {
          // This database is supposed to be a one shard database
          databasesToTest.push(dbname);
        }
      }
      if (databasesToTest.length > 0) {
        for (const [server, {Status, Endpoint}] of Object.entries(health)) {
          if (server.startsWith("PRMR-") && Status === "GOOD") {
            print("INFO Testing Server '" + server);
            arango.reconnect(Endpoint, "_system");
            for (const vocbase of databasesToTest) {
              db._useDatabase(vocbase);
              const {sharding} = db._properties();
              if (sharding !== "single") {
                info.oneShardIncensistencyDetected.add(vocbase);
              }
            }
          }
        }
      }
    }
  };

  const printInconsistentOneShardDatabases = (info) => {
    if (info.oneShardIncensistencyDetected.size > 0) {
      printBad('Your cluster has inconsistencies in DOCUMENT Aql call.');
      const table = new AsciiTable('Databases with DOCUMENT calls that may not find data');
      table.setHeading('Database');
      for (const db of info.oneShardIncensistencyDetected) {
        table.addRow(db);
      }
      print(table.toString());
      return true;
    } else {
      printGood('Your cluster has consistent DOCUMENT calls');
      return false;
    }
  };

  const printDistributionGroups = (info) => {
    const {noInsyncAndDeadLeader} = info;
    let infected = false;
    if (noInsyncAndDeadLeader && noInsyncAndDeadLeader.size > 0) {
      printBad('Your cluster has collections with dead leader and no insync follower');

      const table = new AsciiTable('Collections with deadLeader and no-insync Follower');
      table.setHeading('CID', 'Shard', 'DistributeLike');
      for (const {cid, shard, search} of noInsyncAndDeadLeader) {
        table.addRow(cid, shard, search);
      }
      print(table.toString());
      infected = true;
    } else {
      printGood('Your cluster does not have any collections with dead leader and no insync follower');
    }
    return infected;
  };

  const saveDistributionGroups = (info) => {
    const {noInsyncAndDeadLeader, shardGroups, primaries} = info;
    if (noInsyncAndDeadLeader && noInsyncAndDeadLeader.size > 0) {
      const clonedGroups = new Map();
      for (const {cid, shard, search} of noInsyncAndDeadLeader) {
        // candidates: server => [insyncShard]
        const candidates = new Map();

        const group = shardGroups.get(search);
        const myPlan = group.plan.get(cid);
        const shardIndex = myPlan.findIndex(s => s.shard === shard);
        const allShards = [];
        for (const s of myPlan[shardIndex].servers) {
          if (_.has(primaries, s)) {
            // This primary is alive, let us check
            candidates.set(s, []);
          }
        }
        // Iterate over all current distributions on shardIndex, and if we find an insync follower
        // note it to the candidates
        for (const [cid, curServers] of group.current) {
          const {shard, servers} = curServers[shardIndex];
          allShards.push(shard);
          for (const [c, list] of candidates) {
            if (servers.indexOf(c) !== -1) {
              list.push(shard);
            }
          }
        }

        const sortedCandidates = [...candidates.entries()].sort((l, r) => l[1].length > r[1].length);
        print("List of potential failover candidates, first has most in sync:");
        for (const [c, list] of sortedCandidates) {
          const missing = _.without(allShards, ...list);
          print(`Failover to ${c} insync: ${JSON.stringify(list)}, please check state of ${JSON.stringify(missing)}`);
          print("If you want to failover to this server run the `force-forceover` task against the leader AGENT, e.g.:");
          print(` ./maintenance.sh <options> force-failover ${fs.makeAbsolute('forceFailover.json')} ${c} ${search} ${shardIndex}`);
        }
        clonedGroups.set(search, shardGroups.get(search));
      }
      fs.write("forceFailover.json", JSON.stringify(recursiveMapPrinter(clonedGroups)));
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
      fs.write("collection-integrity.json", JSON.stringify({
        noPlanDatabases,
        noShardCollections,
        realLeaderMissing,
        leaderOnDeadServer,
        followerOnDeadServer
      }));
    }
    if (followerOnDeadServer.length > 0) {
      print("To remedy the dead follower issue please run the task `remove-failed-followers` against the leader AGENT, e.g.:");
      print(` ./maintenance.sh <options> remove-failed-followers ${fs.makeAbsolute('collection-integrity.json')}`);
      print();
    }
  };

  const extractUnplannedFailoverCandidates = (info, dump) => {
    const planCollections = dump.arango.Plan.Collections;
    const currentCollections = dump.arango.Current.Collections;
    const fixes = [];
    for (const [dbname, database] of Object.entries(currentCollections)) {
      for (const [cid, collection] of Object.entries(database)) {
        Object.keys(collection).forEach(function (shname) {
          const shard = collection[shname];
          const candidates = shard.failoverCandidates;
          const planned = planCollections[dbname][cid].shards[shname];
          const cname = planCollections[dbname][cid].name;
          const plannedCandidates = candidates.filter(function (c) {
            return _.indexOf(planned, c) >= 0;
          });

          if (candidates.length !== plannedCandidates.length) {
            fixes.push({
              dbname,
              cid,
              cname,
              shname,
              old: candidates,
              correct: plannedCandidates,
              plan: planned
            });
          }
        });
      }
    }
    info.unplannedFailoverCandidates = fixes;
  };

  const printUnplannedFailoverCandidates = (info, dump) => {
    if (info.unplannedFailoverCandidates.length === 0) {
      printGood('Your cluster does not have any unplanned failover candidates');
      return false;
    }

    printBad('Your cluster has some unplanned failover candidates');

    const table = new AsciiTable('Unplanned Failover Candidates');
    table.setHeading('Database', 'Collections', 'Shard');

    _.each(info.unplannedFailoverCandidates, function (fix) {
      table.addRow(fix.dbname, fix.cname, fix.shname);
    });

    print(table.toString());
    print();

    return true;
  };

  const saveUnplannedFailoverCandidates = (info, dump) => {
    if (info.unplannedFailoverCandidates.length > 0) {
      fs.write("unplanned-failover.json", JSON.stringify(info.unplannedFailoverCandidates));
      print("To remedy the unplanned failover candidates please run the task " +
            "`repair-unplanned-failover` AGAINST AN AGENT, e.g.:");
      print(` ./maintenance.sh <options> repair-unplanned-failover ${fs.makeAbsolute('unplanned-failover.json')}`);
      print();
    }
  };

  const printDatabases = function (info) {
    const table = new AsciiTable('Databases');
    table.setHeading('', 'collections', 'shards', 'leaders', 'followers', 'Real-Leaders');

    _.each(_.sortBy(info.databases, x => x.name), function (database, name) {
      table.addRow(database.name, database.collections.length, database.shards.length,
        database.leaders.length, database.followers.length,
        database.realLeaders.length);
    });

    print(table.toString());
    return false;
  };

  const printCollections = function (info) {
    const table = new AsciiTable('collections');
    table.setHeading('', 'CID', 'RF', 'Shards Like', 'Shards', 'Type', 'Smart');

    const rfs = {};

    _.each(info.collections, function (collection, name) {
      if (!collection.distributeShardsLike) {
        rfs[collection.id] = collection.replicationFactor;
      }
    });

    _.each(_.sortBy(info.collections, x => x.fullName), function (collection, name) {
      let rf = collection.replicationFactor;

      if (collection.distributeShardsLike) {
        rf = "[" + rfs[collection.distributeShardsLike] + "]";
      }

      table.addRow(collection.fullName, collection.id, rf,
        collection.distributeShardsLike, collection.numberOfShards,
        collection.type, collection.isSmart);
    });
    print(table.toString());
    return false;
  };

  const printPrimaryShards = function (info) {
    const table = new AsciiTable('Primary Shards');
    table.setHeading('', 'Leaders', 'Followers', 'Real Leaders');

    _.each(info.shardsPrimary, function (shards, dbServer) {
      table.addRow(dbServer, shards.leaders.length, shards.followers.length, shards.realLeaders.length);
    });

    print(table.toString());
    return false;
  };

  const printZombies = function (info) {
    if (info.zombies.length > 0) {
      printBad('Your cluster has some zombies');
      const table = new AsciiTable('Zombies');
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

  const saveZombies = function (info) {
    if (info.zombies.length > 0) {
      const output = [];

      _.each(info.zombies, function (zombie) {
        output.push({database: zombie.database, cid: zombie.cid, data: zombie.data});
      });

      fs.write("zombies.json", JSON.stringify(output));
      print("To remedy the zombies issue please run the task `remove-zombies` against the leader AGENT, e.g.:");
      print(` ./maintenance.sh <options> remove-zombies ${fs.makeAbsolute('zombies.json')}`);
      print();
    }
  };

  const saveZombieCoords = function (info) {
    if (info.zombieCoordinators.length > 0) {
      fs.write("zombie-coordinators.json", JSON.stringify(info.zombieCoordinators));
      print("To remedy the zombie coordinators issue please run the task `remove-zombie-coordinators` against the leader AGENT, e.g.:");
      print(` ./maintenance.sh <options> remove-zombie-coordinators ${fs.makeAbsolute('zombie-coordinators.json')}`);
      print();
    }
  };

  const saveZombieAnalyzerRevisions = function (info) {
    if (info.zombieAnalyzerRevisions.length > 0) {
      fs.write("zombie-analyzer-revisions.json", JSON.stringify(info.zombieAnalyzerRevisions));
      print("To remedy the zombie analyzer revisions issue please run the task `remove-zombie-analyzer-revisions` against the leader AGENT, e.g.:");
      print(` ./maintenance.sh <options> remove-zombie-analyzer-revisions ${fs.makeAbsolute('zombie-analyzer-revisions.json')}`);
      print();
    }
  };

  const saveCleanedFailoverCandidates = function (info) {
    if (Object.keys(info.correctFailoverCandidates).length > 0) {
      fs.write("cleaned-failovers.json", JSON.stringify(info.correctFailoverCandidates));
      print("To remedy the cleaned out failover db servers issue please run the task `remove-cleaned-failovers` against the leader AGENT, e.g.:");
      print(` ./maintenance.sh <options> remove-cleaned-failovers ${fs.makeAbsolute('cleaned-failovers.json')}`);
      print();
    }
  };

  const printBroken = function (info) {
    if (info.broken.length > 0) {
      printBad('Your cluster has broken collections');
      const table = new AsciiTable('Broken');
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

  const extractCurrentDatabasesDeadPrimaries = (info, dump) => {
    const databases = [];

    _.each(dump.arango.Current.Databases, function (database, name) {
      _.each(database, function (primary, pname) {
        if (!_.has(info.primaries, pname)) {
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

  const printCurrentDatabasesDeadPrimaries = function (info) {
    if (info.databasesDeadPrimaries.length > 0) {
      printBad('Your cluster has dead primaries in Current');
      const table = new AsciiTable('Dead primaries in Current');
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

  const saveCurrentDatabasesDeadPrimaries = function (info) {
    if (info.databasesDeadPrimaries.length > 0) {
      const output = [];

      _.each(info.databasesDeadPrimaries, function (zombie) {
        output.push({database: zombie.database, primary: zombie.primary, data: zombie.data});
      });

      fs.write("dead-primaries.json", JSON.stringify(output));
      print("To remedy the dead primaries issue please run the task `remove-dead-primaries` against the leader AGENT, e.g.:");
      print(` ./maintenance.sh <options> remove-dead-primaries ${fs.makeAbsolute('dead-primaries.json')}`);
      print();
    }
  };

  const extractEmptyDatabases = function (info) {
    info.emptyDatabases = [];
    _.each(_.sortBy(info.databases, x => x.name), function (database, name) {
      if (database.collections.length === 0 && database.shards.length === 0) {
        info.emptyDatabases.push(database);
      }
    });
  };

  const printEmptyDatabases = function (info) {
    if (info.emptyDatabases.length > 0) {
      printBad('Your cluster has some skeleton databases (databases without collections)');
      const table = new AsciiTable('Skeletons');
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

  const saveEmptyDatabases = function (info) {
    if (info.emptyDatabases.length > 0) {
      const output = [];

      _.each(info.emptyDatabases, function (skeleton) {
        output.push({database: skeleton.name, data: skeleton.data});
      });

      fs.write("skeleton-databases.json", JSON.stringify(output));
      print("To remedy the skeleton databases issue please run the task `remove-skeleton-databases` against the leader AGENT, e.g.:");
      print(` ./maintenance.sh <options> remove-skeleton-databases ${fs.makeAbsolute('skeleton-databases.json')}`);
      print();
    }
  };

  const extractMissingCollections = function (info) {
    info.missingCollections = [];

    _.each(_.sortBy(info.databases, x => x.name), function (database, name) {
      const system = database.collections.filter(function (c) {
        return c.name[0] === '_';
      }).map(function (c) {
        return c.name;
      });

      const missing = [];
      ["_apps", "_appbundles", "_aqlfunctions", "_graphs", "_jobs", "_queues"].forEach(function (name) {
        if (system.indexOf(name) === -1) {
          missing.push(name);
        }
      });

      if (missing.length > 0) {
        info.missingCollections.push({database: database.name, missing});
      }
    });
  };

  const printMissingCollections = function (info) {
    if (info.missingCollections.length > 0) {
      printBad('Your cluster is missing relevant system collections:');
      const table = new AsciiTable('Missing collections');
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

  const saveMissingCollections = function (info) {
    if (info.missingCollections.length > 0) {
      const output = info.missingCollections;

      fs.write("missing-collections.json", JSON.stringify(output));
      print("To remedy the missing collections issue please run the task " +
            "`create-missing-collections` AGAINST A COORDINATOR, e.g.:");
      print(` ./maintenance.sh <options> create-missing-collections ${fs.makeAbsolute('missing-collections.json')}`);
      print();
    }
  };

  const extractCleanedFailoverCandidates = (info, dump) => {
    const currentCollections = dump.arango.Current.Collections;
    const cleanedServers = _.uniq(_.concat(dump.arango.Target.CleanedServers, Object.keys(dump.arango.Target.FailedServers)));
    const fixes = {};
    Object.keys(currentCollections).forEach(function (dbname) {
      const database = dump.arango.Current.Collections[dbname];
      Object.keys(database).forEach(function (colname) {
        const collection = database[colname];
        Object.keys(collection).forEach(function (shname) {
          const shard = collection[shname];
          const inter = _.intersectionWith(cleanedServers, shard.failoverCandidates);
          let left = shard.failoverCandidates;
          left = _.difference(left, inter);
          if (inter.length > 0) {
            const n = "arango/Current/Collections/" + dbname + "/" +
                  colname + "/" + shname + "/failoverCandidates";
            fixes[n] = [left, shard.failoverCandidates];
          }
        });
      });
    });
    info.correctFailoverCandidates = fixes;
  };

  const extractOutOfSyncFollowers = (info, dump) => {
    const planCollections = dump.arango.Plan.Collections;
    const currentCollections = dump.arango.Current.Collections;
    const compareFollowers = (plan, current) => {
      // If leaders are not equal we are out of sync.
      if (plan[0] !== current[0]) {
        return false;
      }
      if (plan.length === 1) {
        // we have not even requested a follower
        return true;
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
      if (!_.has(currentCollections, db)) {
        // database skeleton or  so, don't care
        continue;
      }
      for (const [name, col] of Object.entries(collections)) {
        const {shards} = col;
        if (!shards || Object.keys(shards).length === 0) {
          continue;
        }
        for (const [shard, servers] of Object.entries(shards)) {
          try {
            const current = currentCollections[db][name][shard].servers;
            if (!compareFollowers(servers, current)) {
              info.outOfSyncFollowers.push({
                db, name, shard, servers, current
              });
            }
          } catch (e) {}
        }
      }
    }
  };

  const printOutOfSyncFollowers = (info) => {
    const {outOfSyncFollowers} = info;
    const counters = new Map();
    if (outOfSyncFollowers.length > 0) {
      printBad('Your cluster has collections where followers are out of sync');
      {
        const table = new AsciiTable('Out of sync followers');
        table.setHeading('Database', 'CID', 'Shard', 'Planned', 'Real');
        for (const oosFollower of outOfSyncFollowers) {
          table.addRow(oosFollower.db, oosFollower.name, oosFollower.shard, oosFollower.servers, oosFollower.current);
          counters.set(oosFollower.servers[0], (counters.get(oosFollower.servers[0]) || 0) + 1);
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
      printGood('Your cluster does not have collections where followers are out of sync');
      return false;
    }
  };

  const extractBrokenEdgeIndexes = (info, dump) => {
    info.brokenEdgeIndexes = [];
    const planCollections = dump.arango.Plan.Collections;
    for (const [db, collections] of Object.entries(planCollections)) {
      for (const [name, col] of Object.entries(collections)) {
        const {indexes} = col;
        if (!indexes || Object.keys(indexes).length === 0) {
          continue;
        }
        let failed = false;
        const newIndexes = [];
        for (const [pos, index] of Object.entries(indexes)) {
          if (index.type === "edge" &&
              index.name === "edge" &&
              index.id === "1" &&
              index.fields.length > 1) {
            failed = true;
          }

          if (index.id === "1") {
            newIndexes.push({
              id: "1",
              type: "edge",
              name: "edge",
              fields: ["_from"],
              unique: false,
              sparse: false
            });
            newIndexes.push({
              id: "2",
              type: "edge",
              name: "edge",
              fields: ["_to"],
              unique: false,
              sparse: false
            });
          } else if (index.id !== "2") {
            newIndexes.push(index);
          }
        }
        if (failed) {
          info.brokenEdgeIndexes.push({
            path: "/Plan/Collections/" + db + "/" + name + "/indexes",
            bad: indexes,
            good: newIndexes
          });
        }
      }
    }
  };

  const printBrokenEdgeIndexes = (info) => {
    const {brokenEdgeIndexes} = info;
    if (brokenEdgeIndexes.length > 0) {
      printBad('Your cluster has broken edge indexes');
      return true;
    } else {
      printGood('Your cluster does not have broken edge indexes');
      return false;
    }
  };

  const saveBrokenEdgeIndexes = function (info) {
    const {brokenEdgeIndexes} = info;
    if (brokenEdgeIndexes.length > 0) {
      fs.write("broken-edge-indexes.json", JSON.stringify(brokenEdgeIndexes));
      print("To remedy the broken-edge-index issue please run the task " +
            "`repair-broken-edge-indexes` AGAINST A COORDINATOR, e.g.:");
      print(` ./maintenance.sh <options> repair-broken-edge-indexes ${fs.makeAbsolute('broken-edge-indexes.json')}`);
      print();
    }
  };

  const extractShardingStrategy = (info, dump) => {
    const planCollections = dump.arango.Plan.Collections;
    const planDBs = dump.arango.Plan.Databases;
    info.shardingStrategy = [];
    for (const [db, collections] of Object.entries(planCollections)) {
      if (!_.has(planDBs, db)) {
        // This database has Collections but is deleted.
        info.noPlanDatabases.push(db, collections);
        continue;
      }
      for (const [cid, col] of Object.entries(collections)) {
        const {name, type, shardingStrategy, isSmart} = col;
        if (shardingStrategy) {
          continue;
        }
        let newStrategy;
        if (type === 2 || !isSmart) {
          newStrategy = "enterprise-compat";
        } else if (type === 3 && isSmart) {
          newStrategy = "enterprise-smart-edge-compat";
        }
        if (!newStrategy) {
          continue;
        }
        info.shardingStrategy.push({
          database: db,
          cid: cid,
          name: name,
          newStrategy: newStrategy
        });
      }
    }
  };

  const printShardingStrategy = (info) => {
    const {shardingStrategy} = info;
    if (shardingStrategy.length > 0) {
      printBad('Your cluster has sharding strategies that need fixing');
      return true;
    } else {
      printGood('Your cluster does correct sharding strategies');
      return false;
    }
  };

  const saveShardingStrategy = function (info) {
    const {shardingStrategy} = info;
    if (shardingStrategy.length > 0) {
      fs.write("sharding-strategy.json", JSON.stringify(shardingStrategy));
      print("To remedy the sharding-strategy issue please run the task " +
            "`repair-sharding-strategy` AGAINST AN AGENT, e.g.:");
      print(` ./maintenance.sh <options> repair-sharding-strategy ${fs.makeAbsolute('sharding-strategy.json')}`);
      print();
    }
  };

  const info = {};

  if (stores !== undefined) {
    info.callbacks = stores.read_db[2];
  }
  // If we have parsed a file, we are not on a live server
  info.__usesLiveServer = !parsedFile;

  // extract info
  extractFailed(info, dump);
  helper.extractPrimaries(info, dump);
  helper.extractDatabases(info, dump);
  zombieCoordinators(info, dump);
  zombieAnalyzerRevisions(info, dump);
  extractCollectionIntegrity(info, dump);
  extractCurrentDatabasesDeadPrimaries(info, dump);
  extractDistributionGroups(info, dump);
  extractEmptyDatabases(info);
  extractMissingCollections(info);
  extractOutOfSyncFollowers(info, dump);
  extractCleanedFailoverCandidates(info, dump);
  extractBrokenEdgeIndexes(info, dump);
  extractShardingStrategy(info, dump);
  extractUnplannedFailoverCandidates(info, dump);
  extractInconsistentOneShardDatabases(info, dump);

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
  infected = printZombieCoordinators(info) || infected;
  infected = printZombieAnalyzerRevisions(info) || infected;
  infected = printCleanedFailoverCandidates(info) || infected;
  infected = printBroken(info) || infected;
  infected = printCollectionIntegrity(info) || infected;
  infected = printCurrentDatabasesDeadPrimaries(info) || infected;
  infected = printEmptyDatabases(info) || infected;
  infected = printMissingCollections(info) || infected;
  infected = printOutOfSyncFollowers(info) || infected;
  infected = printDistributionGroups(info) || infected;
  infected = printBrokenEdgeIndexes(info) || infected;
  infected = printShardingStrategy(info) || infected;
  infected = printUnplannedFailoverCandidates(info) || infected;
  infected = printInconsistentOneShardDatabases(info) || infected;
  print();

  if (infected) {
    // Save to files
    saveCollectionIntegrity(info);
    saveZombies(info);
    saveZombieCoords(info);
    saveZombieAnalyzerRevisions(info);
    saveCurrentDatabasesDeadPrimaries(info);
    saveDistributionGroups(info);
    saveEmptyDatabases(info);
    saveMissingCollections(info);
    saveCleanedFailoverCandidates(info);
    saveBrokenEdgeIndexes(info);
    saveShardingStrategy(info);
    saveUnplannedFailoverCandidates(info);
  } else {
    printGood('Did not detect any issues in your cluster');
  }

  saveZombieCallbacks(info, stores);
};
