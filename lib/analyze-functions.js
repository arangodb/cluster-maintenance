/* jshint globalstrict:false, strict:false, sub: true */
/**
 * Pure analysis functions extracted from analyze.js for testability.
 * These functions take an info object and dump object and populate info with analysis results.
 * They have no side effects (no printing, no file writing, no network calls).
 */

const _ = require('lodash');

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
        for (let i = 0; i < servers.length; ++i) {
          if (!_.has(info.primaries, servers[i])) {
            if (i === 0) {
              info.leaderOnDeadServer.push({db, name, shard, server: servers[i], servers});
            } else {
              info.followerOnDeadServer.push({db, name, shard, server: servers[i], servers});
            }
          }
        }
      }
    }
  }

  for (const [db, collections] of Object.entries(currentCollections)) {
    for (const [name, col] of Object.entries(collections)) {
      for (const [shard, desc] of Object.entries(col)) {
        if (desc.hasOwnProperty("errorNum") && desc.hasOwnProperty("errorMessage") &&
            desc.errorNum > 0) {
          // current shard only contains an error message
          continue;
        }
        
        const servers = desc.servers;
        for (let i = 0; i < servers.length; ++i) {
          if (!_.has(info.primaries, servers[i])) {
            if (i === 0) {
              info.leaderOnDeadServer.push({db, name, shard, server: servers[i], currentServers: servers});
            } else {
              info.followerOnDeadServer.push({db, name, shard, server: servers[i], currentServers: servers});
            }
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

const extractRevisionIdIssues = (info, dump) => {
  info.revisionIdIssues = [];
  const planCollections = dump.arango.Plan.Collections;

  const latestId = dump.arango.Sync.LatestID;
  
  for (const [db, collections] of Object.entries(planCollections)) {
    for (const [name, col] of Object.entries(collections)) {
      const {type, isSmart, usesRevisionsAsDocumentIds} = col;

      // only check edge collections
      if (!isSmart || (type !== 3 && type !== "3" && type !== "edge")) {
        continue;
      }

      if (!usesRevisionsAsDocumentIds) {
        continue;
      }

      info.revisionIdIssues.push({ db, collection: col.name, latestId });
    }
  }
};

const extractSatelliteIssues = (info, dump) => {
  info.satelliteIssues = [];
  const planCollections = dump.arango.Plan.Collections;

  for (const [db, collections] of Object.entries(planCollections)) {
    for (const [cid, col] of Object.entries(collections)) {
      const {isSmart, replicationFactor} = col;

      // look for replicationFactor === 0. should be "satellite" instead.
      if (isSmart || replicationFactor !== 0) {
        continue;
      }
        
      info.satelliteIssues.push({ cid, database: db, collection: col.name });
    }
  }
};

const extractSupervisionLocks = (info, dump) => {
  info.supervisionLocks = [];
  if (typeof(dump.arango.Supervision.DBServers) === "object") {
    for (let dbserver in dump.arango.Supervision.DBServers) {
      let lock = dump.arango.Supervision.DBServers[dbserver];
      if (typeof(lock) === "string") {
        // write lock
        if (!dump.arango.Target.Pending.hasOwnProperty(lock)) {
          // No pending job for this lock, alert!
          info.supervisionLocks.push({type:"DBServer write lock", dbserver, job: lock, shard: ''});
        }
      } else if (typeof(lock) === "object" && Array.isArray(lock)) {
        // read locks
        for (let job of lock) {
          if (!dump.arango.Target.Pending.hasOwnProperty(job)) {
            info.supervisionLocks.push({type:"DBServer read lock", dbserver, job, shard: ''});
          }
        }
      }
    }
  }
  if (typeof(dump.arango.Supervision.Shards) === "object") {
    for (let shard in dump.arango.Supervision.Shards) {
      let lock = dump.arango.Supervision.Shards[shard];
      if (typeof(lock) === "string") {
        // shard is locked
        if (!dump.arango.Target.Pending.hasOwnProperty(lock)) {
          info.supervisionLocks.push({"type":"Shard lock", shard, job: lock, dbserver: ''});
        }
      }
    }
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

const extractEmptyDatabases = function (info) {
  info.emptyDatabases = [];
  _.each(_.sortBy(info.databases, x => x.name), function (database, name) {
    if (database.collections.length === 0 && database.shards.length === 0) {
      info.emptyDatabases.push(database);
    }
  });
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

const extractUnplannedFailoverCandidates = (info, dump) => {
  const planCollections = dump.arango.Plan.Collections;
  const currentCollections = dump.arango.Current.Collections;
  const fixes = [];
  for (const [dbname, database] of Object.entries(currentCollections)) {
    for (const [cid, collection] of Object.entries(database)) {
      Object.keys(collection).forEach(function (shname) {
        try {
          const shard = collection[shname];
          const candidates = shard.failoverCandidates;
          if (planCollections[dbname][cid] === undefined) {
            // Collection is already deleted in the plan, let's ignore it here!
            return;
          }
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
        } catch (e) {
          print(`ERROR: corruption in db ${dbname} cid ${cid} shard ${shname}, error: ${e}`);
        }
      });
    }
  }
  info.unplannedFailoverCandidates = fixes;
};

// Export all functions
module.exports = {
  extractFailed,
  zombieCoordinators,
  zombieAnalyzerRevisions,
  recursiveMapPrinter,
  extractCollectionIntegrity,
  extractDistributionGroups,
  extractRevisionIdIssues,
  extractSatelliteIssues,
  extractSupervisionLocks,
  extractCurrentDatabasesDeadPrimaries,
  extractEmptyDatabases,
  extractMissingCollections,
  extractCleanedFailoverCandidates,
  extractOutOfSyncFollowers,
  extractBrokenEdgeIndexes,
  extractShardingStrategy,
  extractUnplannedFailoverCandidates
};

