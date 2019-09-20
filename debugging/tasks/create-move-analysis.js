/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango, db */
exports.name = "create-move-analysis";
exports.group = "analyse move shard tasks";
exports.args = [
  {"name": "dump-file", "optional": true, "type": "jsonfile", "description": "agency dump file"}
];
exports.args_arangosh = "| --server.endpoint LEADER-AGENT";
exports.description = "Creates analysis for a plan to rebalance shards in your cluster.";
exports.selfTests = ["arango", "db"];
exports.requires = "3.3.23 - 3.5.99";
exports.info = `
This task creates operations that can be applied to rebalance shards in a
cluster that has become inbalanced due to server failures.

Note: This task has the implicit assumption that there is one
DBServer with few shards and two with many (e.g. 1000, 1000, 10000). 
If there are two DBServers with lower than average
number of shards, you would need to run the task twice.

Currently there is a limit of 50k move shard jobs set (due to
JavaScript String limitations).

Execute the analyze shard script (will also create a move plan "moveShardsPlan.json"):
 - arangosh --javascript.execute ../debug-scripts/debugging/index.js  create-move-analysis --server.endpoint tcp://(ip-address):(agency-port)> (agency)
 - arangosh --javascript.execute ../debug-scripts/debugging/index.js  create-move-analysis --server.endpoint agencyDump.json (dump)
`;

exports.run = function (extra, args) {

  // imports
  const helper = require('../helper.js');
  const fs = require('fs');
  const _ = require('underscore');

  // variables
  const file = helper.getValue("dump-file", args);
  const dump = helper.getAgencyDumpFromObjectOrAgency(file);
  const agencyPlan = dump.arango.Plan;
  // const agencyDatabases = agencyPlan.Databases;
  const initAgencyCollections = agencyPlan.Collections;
  const health = dump.arango.Supervision.Health;

  // statics
  const MIN_ALLOWED_SCORE = 0.9;
  const MAX_ITERATIONS = 2;

  // Analysis Data Format
  // { // TODO HEIKO update that one here
  //   leaderCollectionA: {
  //     followerCollectionX: {
  //       s12: {nodes: [leader, follower1, follower2, ... ]}
  //       s13: {nodes: [leader, follower1, follower2, ... ]}
  //     }
  //   },
  //   leaderCollectionB: {},
  //    ...
  // }

  // here we will store our current cluster state
  // start globals, keep that small.

  // collection id -> collection name
  let collectionNamesMap = {};

  // collection name -> collection replication factor
  let collectionReplicationFactorMap = {};

  // a list of buckets (distributeShardLikes shard containers)
  // mainCollection -> collections (distributeShardsLike)
  let shardBucketList = {};

  // a local list of jobs we did to optimize the plan
  let jobHistory = [];

  // end globals

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

  // start helper functions
  let getCountOfCurrentDatabaseServers = function () {
    let count = 0;
    _.each(health, function (info, serverName) {
      if (serverName.substring(0, 4) === 'PRMR') {
        // found dbserver
        if (info.Status === 'GOOD') {
          count++;
        }
      }
    });
    return count;
  };
  let getNamesOfCurrentDatabaseServers = function () {
    let names = [];
    _.each(health, function (info, serverName) {
      if (serverName.substring(0, 4) === 'PRMR') {
        // found dbserver
        if (info.Status === 'GOOD') {
          names.push(serverName);
        }
      }
    });
    return names;
  };

  let isSystemCollection = function (collectionName) {
    if (collectionName.charAt(0) === '_') {
      if (collectionName.substring(0, 3) === '_to_') {
        return false;
      } else if (collectionName.substring(0, 6) === '_from_') {
        return false;
      } else if (collectionName.substring(0, 7) === '_local_') {
        return false;
      } else {
        return true;
      }
    }
    return false;
  };

  let buildCollectionNamesMap = function () {
    _.each(initAgencyCollections, function (collections) {
      _.each(collections, function (collection, cId) {
        collectionNamesMap[cId] = collection.name;
        collectionReplicationFactorMap[collection.name] = collection.replicationFactor;
      });
    });
  };

  let buildShardDistribution = function (collection, databaseName, distributeShardsLike) {
    let cObj = {};
    _.each(collection.shards, function (distribution, shardId) {
      cObj[shardId] = {};
      // cObj[shardId].nrFollowers = distribution.length;
      cObj[shardId].distribution = distribution;
      if (distributeShardsLike) {
        cObj[shardId].distributeShardsLike = distributeShardsLike;
      }
    });

    return cObj;
  };

  let addLeaderCollection = function (collection, databaseName) {
    if (isSystemCollection(collection.name)) {
      // skipping system collections
      return;
    }

    let sharding = buildShardDistribution(collection, databaseName);
    if (Object.keys(sharding).length > 0) {
      return sharding;
    } else {
      print("Debug: Empty sharding for collection " + collection.name)
      return;
    }
  };

  // a "follower collection" has distributeShardsLike, will be maintained in buckets
  let addFollowerCollection = function (collection, databaseName) {
    let distributeShardsLike = collectionNamesMap[collection.distributeShardsLike];
    if (isSystemCollection(distributeShardsLike)) {
      // skipping system collections
      return;
    }

    if (!distributeShardsLike) {
      // invalid state
      return;
    }

    if (collection.numberOfShards === 0) {
      // TODO: check for more edge cases
      // e.g. shadow collection
      print("Debug - Skipped collection: " + collection.name);
      if (collection.shadowCollections && collection.shadowCollections.length > 0) {
        print("Debug - Reason: Shadow collection");
        //_.each(collection.shadowCollections, function (shadowCollectionID){
        //  print(collectionNamesMap[shadowCollectionID]);
        // });
      } else {
        print("Debug - Reason: Unknown.")
        print(collection);
      }
      return;
    }
    if (!shardBucketList[databaseName]) {
      shardBucketList[databaseName] = {};
    }

    if (!shardBucketList[databaseName][distributeShardsLike]) {
      shardBucketList[databaseName][distributeShardsLike] = {
        followers: [],
        leaderDB: distributeShardsLike,
        replicationFactor: collectionReplicationFactorMap[distributeShardsLike],
        numberOfShards: collection.numberOfShards,
        shardCollectionTotalAmount: collectionReplicationFactorMap[distributeShardsLike] * collection.numberOfShards
      };
    }
    if (!shardBucketList[databaseName][distributeShardsLike].followers.includes(collection.name)) {
      shardBucketList[databaseName][distributeShardsLike].followers.push(collection.name);
    }
  };

  // calculate some environment properties here
  let info = {
    amountOfDatabaseServers: getCountOfCurrentDatabaseServers(),
    dbServerNames: getNamesOfCurrentDatabaseServers()
  };

  // calculate helper methods
  let calculateShardDistributionInformation = function (totalShards, collectionName, leaders, followers) {
    let multipliedTotalShards = totalShards * collectionReplicationFactorMap[collectionName];
    let perfectAmountOfLeaders = Math.round(totalShards / info.amountOfDatabaseServers);
    let perfectAmountOfShards = multipliedTotalShards / info.amountOfDatabaseServers;

    return {
      perfectAmountOfShards: perfectAmountOfShards,
      perfectAmountOfLeaders: perfectAmountOfLeaders,
      perfectAmountOfFollowers: perfectAmountOfShards - perfectAmountOfLeaders,
      upperBound: Math.ceil(multipliedTotalShards / info.amountOfDatabaseServers),
      lowerBound: Math.floor(multipliedTotalShards / info.amountOfDatabaseServers),
      shardLeaderAmount: leaders,
      shardFollowerAmount: followers,
      shardTotalAmount: leaders + followers
    }
  };

  let calulateCollectionScore = function (analysisData, collectionName, dbServerName) {
    // tries to calculate the distribution based on collection shards
    // TODO: Next step will also be to verify the distribution regarding leader <-> follower
    // No server should have leaders/followers only.
    // skip collections which do have distributeShardsLike (calculation differs)

    let score = -1;
    let leaders = 0;
    let followers = 0;
    let totalShards = 0;

    _.each(analysisData, function (database, databaseName) {
      _.each(database[collectionName], function (shard) {
        if (shard.distribution[0] === dbServerName) {
          leaders++;
          totalShards++;
        } else {
          if (shard.distribution.indexOf(dbServerName) > 0) {
            followers++;
            totalShards++;
          }
        }
      });
    });

    let shardDistributeInfo = calculateShardDistributionInformation(
      totalShards, collectionName, leaders, followers
    );

    let shardsWeHave = shardDistributeInfo.shardTotalAmount;
    if (shardsWeHave >= shardDistributeInfo.lowerBound && shardsWeHave <= shardDistributeInfo.upperBound && shardsWeHave !== 0) {
      // we are in that range of lowerBound <-> upperBound, almost perfect distribution
      score = 1;
    } else if (shardsWeHave == shardDistributeInfo.perfectAmountOfShards && shardsWeHave !== 0) {
      // perfect distribution
      score = 1;
    } else if (shardsWeHave > shardDistributeInfo.perfectAmountOfShards) {
      // we have too much shards, we might need to remove some shards
      let shardsWeHaveTooMuch = shardsWeHave - shardDistributeInfo.perfectAmountOfShards;
      score = 1 - ((shardDistributeInfo.perfectAmountOfShards / shardsWeHaveTooMuch) / 10);
    } else if (shardsWeHave < shardDistributeInfo.perfectAmountOfShards && shardsWeHave !== 0) {
      // we have less then perfect shards, we might need fill that one up
      score = 1.0 / (shardDistributeInfo.perfectAmountOfShards - shardsWeHave);
    } else if (shardsWeHave == 0) {
      // we do not have any shards
      score = 0;
    }

    return [score, shardDistributeInfo];
  };

  let calculateCollectionsScore = function (analysisData) {
    let score = {};
    _.each(info.dbServerNames, function (dbServerName) {
      score[dbServerName] = {};

      _.each(analysisData, function (database, databaseName) {
        _.each(database, function (collection, collectionName) { // analysisData original
          let info = calulateCollectionScore(analysisData, collectionName, dbServerName);
          let localScore = info[0];
          let distribution = info[1];

          // prepare empty objects
          score[dbServerName] = {};
          score[dbServerName][databaseName] = {};

          score[dbServerName][databaseName][collectionName] = {
            score: localScore,
            distribution: distribution
          };
        });
      });
    });
    return score;
  };

  let generateAnalysis = function (agencyCollections) {
    resultData = {};

    _.each(agencyCollections, function (collections, databaseName) {
      _.each(collections, function (collection) {

        if (isSystemCollection(collection.name)) {
          print("SKIPPED: " + collection.name); // TODO: RE-ENABLE SYSTEM COLLECTIONS
          return;
        }

        if (collection.distributeShardsLike) {
          // found followers, add them to the bucket
          addFollowerCollection(collection, databaseName);
          /*
          if (fResult !== null && fResult !== undefined) {
            if (!resultData[collection.name]) {
              resultData[collection.name] = {};
            }
            resultData[collection.name] = fResult;
          }*/
        } else {
          // found leaders
          let lResult = addLeaderCollection(collection, databaseName);
          if (lResult !== null && lResult !== undefined) {
            if (!resultData[databaseName]) {
              resultData[databaseName] = {};
              resultData[databaseName][collection.name] = {};
            }
            resultData[databaseName][collection.name] = lResult;
          }
        }
      });
    });
    return resultData;
  };

  // function to calculate total amount of shards inside a bucket group
  let extendShardBucketList = function () {
    _.each(shardBucketList, function (database) {
      _.each(database, function (info) {
        info.shardBucketTotalAmount = info.followers.length * info.numberOfShards * info.replicationFactor;
      });
    });
  };

  let isBucketMaster = function (collectionName) {
    if (shardBucketList[collectionName]) {
      return true;
    } else {
      return false;
    }
  };

  let getCandidatesToOptimize = function (score) {
    /*
     * {
     *   worstDatabaseServer: abc,
     *   bestDatabaseServer: xyz,
     * }
     */

    let candidates = {};
    _.each(score, function (database, databaseServerName) {
      _.each(database, function (collections, databaseName) {
        _.each(collections, function (collection, collectionName) {
          if (collection.score <= MIN_ALLOWED_SCORE) {
            if (!candidates[databaseName]) {
              candidates[databaseName] = {};
            }

            if (!candidates[databaseName][collectionName]) {
              // are we a bucket master?
              let bucketMaster = isBucketMaster(collectionName);

              candidates[databaseName][collectionName] = {
                bestScore: null,
                bestAmountOfLeaders: null,
                bestAmountOfFollowers: null,
                bestDatabaseServer: null,
                perfectAmountOfShards: null,
                weakestScore: null,
                weakestAmountOfLeaders: null,
                weakestAmountOfFollowers: null,
                weakestDatabaseServer: null,
                isBucketMaster: bucketMaster
              };
            }

            if (candidates[databaseName][collectionName].weakestScore === null || collection.score < candidates[databaseName][collectionName].weakestScore) {
              candidates[databaseName][collectionName].weakestScore = collection.score;
              candidates[databaseName][collectionName].weakestDatabaseServer = databaseServerName;
              candidates[databaseName][collectionName].weakestAmountOfLeaders = collection.distribution.shardLeaderAmount;
              candidates[databaseName][collectionName].weakestAmountOfFollowers = collection.distribution.shardFollowerAmount;
              candidates[databaseName][collectionName].perfectAmountOfShards = collection.distribution.perfectAmountOfShards;
              candidates[databaseName][collectionName].perfectAmountOfLeaders = collection.distribution.perfectAmountOfLeaders;
              candidates[databaseName][collectionName].perfectAmountOfFollowers = collection.distribution.perfectAmountOfFollowers;
            }

            if (candidates[databaseName][collectionName].bestScore === null || collection.score > candidates[databaseName][collectionName].bestScore) {
              candidates[databaseName][collectionName].bestScore = collection.score;
              candidates[databaseName][collectionName].bestDatabaseServer = databaseServerName;
              candidates[databaseName][collectionName].bestAmountOfLeaders = collection.distribution.shardLeaderAmount;
              candidates[databaseName][collectionName].bestAmountOfFollowers = collection.distribution.shardFollowerAmount;
              candidates[databaseName][collectionName].perfectAmountOfShards = collection.distribution.perfectAmountOfShards;
              candidates[databaseName][collectionName].perfectAmountOfLeaders = collection.distribution.perfectAmountOfLeaders;
              candidates[databaseName][collectionName].perfectAmountOfFollowers = collection.distribution.perfectAmountOfFollowers;
            }
          }
        });
      });
    });

    return candidates;
  };

  moveSingleShardLocally = function (shardId, fromDBServer, toDBServer,
                                     collectionName, isLeader, analysisData, databaseName) {
    // move shards in our local state only
    // debug:
    // print("Moving: " + shardId + " from: " + fromDBServer + " to: " + toDBServer + "(leader: " + isLeader + ")");

    let success = false;

    if (fromDBServer == toDBServer) {
      print("Best and worst server are equal. No actions needs to be done.")
      // makes no sense to do this
      return {
        success: success,
        data: null
      }
    }

    // modifiy local state
    if (isLeader) {
      // remove old leader, add new one
      if (analysisData[databaseName][collectionName][shardId].distribution.indexOf(toDBServer) > 0) {
        // we are already follower, move not allowed
        print("THIS IS NOT ALLOWED TO HAPPEN")
      } else {
        analysisData[databaseName][collectionName][shardId].distribution.shift();
        analysisData[databaseName][collectionName][shardId].distribution.unshift(toDBServer);
        success = true;
      }
    } else {
      // check that toDBServer is NOT a follower or a leader
      let toDBServerPos = analysisData[databaseName][collectionName][shardId].distribution.indexOf(toDBServer);
      if (toDBServerPos === -1) {
        // we are not a follower or a leader of this shard
        let fromDBServerPos = analysisData[databaseName][collectionName][shardId].distribution.indexOf(fromDBServer);
        if (fromDBServerPos === -1) {
          print("========= BAD STATE ======= ");
        } else {
          analysisData[databaseName][collectionName][shardId].distribution[fromDBServerPos] = toDBServer;
          success = true;
        }
      }
    }

    if (success) {
      // persist action history in local jobHistory
      jobHistory.push({
        // action: "moveShard",
        database: databaseName,
        collection: collectionName,
        shard: shardId,
        fromServer: fromDBServer,
        toServer: toDBServer
      });
    }

    return {
      success: success,
      data: analysisData
    }
  };

  let getTotalAmountOfShards = function (databaseServer) {
    let x = {};
    extractDatabases(x, dump); // TODO: move me to helper function (used in analyze.js)
    extractPrimaries(x, dump); // TODO: move me to helper function (used in analyze.js)
    let shards = x.shardsPrimary[databaseServer];
    return shards.leaders.length + shards.followers.length;
  };

  /* INFO:
  TODO: Replaced by above function. Maybe optimize complete shard calculation later. OR remove this.
  let getTotalAmountOfShards = function (databaseServer, analysisData) {
    let totalShards = 0;

    _.each(analysisData, function(shards, collectionName) {
      if (isBucketMaster(collectionName)) {
        // special calculation
        // check if distributeShardsLike collection is holder of collection
        let found = false;
        _.each(shards, function (shard) {
          if (shard.distribution.includes(databaseServer)) {
            // TODO: Check if that one here is correct, not 100% sure.
            found = true;
          }
        });
        if (found) {
          totalShards = totalShards + (shardBucketList[collectionName].shardBucketTotalAmount / shardBucketList[collectionName].replicationFactor);
        }
      } else {
        _.each(shards, function (shard) {
          if (shard.distribution.includes(databaseServer)) {
            totalShards = totalShards++;
          }
        });
      }
    });

    return totalShards;
  };*/

  // this function will move shards locally around and then return a new state
  let moveShardsLocally = function (candidates, analysisData) {

    // TODO: Currently it is random if a "bucket" or a "single shard" is moved
    // TODO: We should first move buckets around, then single shards!

    // first detect the amount of what (leader/follower) to move
    _.each(candidates, function (database, databaseName) {
      _.each(database, function (stats, collectionName) {
        let amountOfLeadersToMove = 0;
        let amountOfFollowersToMove = 0;
        let moveBucket = false;

        // special condition:
        // if we are a masterBucket collection, we need to take a look at the global
        // shard distribution per database before we start moving.
        if (stats.isBucketMaster) {
          let amountOfTotalShardsOfBestServer = getTotalAmountOfShards(
            stats.bestDatabaseServer, analysisData, true
          );
          let amountOfTotalShardsOfWeakestServer = getTotalAmountOfShards(
            stats.weakestDatabaseServer, analysisData, true
          );
          // print("WE FOUND A BUCKET MASTER !! - Name: " + collectionName);
          // print("Total amount of best: " + amountOfTotalShardsOfBestServer);
          // print("Total amount of worst: " + amountOfTotalShardsOfWeakestServer);

          if (amountOfTotalShardsOfBestServer > amountOfTotalShardsOfWeakestServer) {
            let shardDifference = amountOfTotalShardsOfBestServer - amountOfTotalShardsOfWeakestServer;
            if (shardDifference > shardBucketList[collectionName].shardBucketTotalAmount) {
              // TODO: Move bucket calculation - Check if we could calculate more precise
              moveBucket = true;
            } else {
              // no change - we are not moving the bucket, quick exit: return same state
              return;
            }
          } else {
            // no change - we are not moving the bucket, quick exit: return same state
            return;
          }
        } else {
          // calculate a regular collection
          if (stats.bestAmountOfLeaders > stats.weakestAmountOfLeaders) {
            // we might need to move leaders
            if (stats.bestAmountOfLeaders > stats.perfectAmountOfLeaders) {
              amountOfLeadersToMove = stats.bestAmountOfLeaders - stats.perfectAmountOfLeaders;
            }
          }
          if (stats.bestAmountOfFollowers > stats.perfectAmountOfFollowers) {
            // we need to move followers
            amountOfFollowersToMove = stats.bestAmountOfFollowers - stats.perfectAmountOfFollowers;
          }

          if (amountOfFollowersToMove === 0 && amountOfLeadersToMove === 0) {
            // no change, quick exit: return same state
            return;
          }
        }


        // now iterate through current state and start moving (local only!)
        // TODO: optimization, do not use each, quick exit not possible
        // for (let [databaseName, database] of Object.entries(analysisData))

        _.each(analysisData, function (database, databaseName) {
          _.each(database[collectionName], function (shard, shardId) {
            if (shard.distribution[0] === stats.bestDatabaseServer) {
              // we found the best db server as leader for the current shard
              if (amountOfLeadersToMove > 0 || moveBucket) { // TODO: CHECK exit
                let result = moveSingleShardLocally(
                  shardId, stats.bestDatabaseServer, stats.weakestDatabaseServer,
                  collectionName, true, analysisData, databaseName
                );
                if (result.success) {
                  analysisData = result.data;
                  amountOfLeadersToMove--;
                }
              }
            } else {
              // we might have a follower shard
              if (shard.distribution.indexOf(stats.bestDatabaseServer) > 0) {
                // we found dbserver as follower
                if (amountOfFollowersToMove > 0 || moveBucket) { // TODO: CHECK exit
                  let result = moveSingleShardLocally(
                    shardId, stats.bestDatabaseServer, stats.weakestDatabaseServer,
                    collectionName, false, analysisData, databaseName
                  );
                  if (result.success) {
                    analysisData = result.data;
                    amountOfFollowersToMove--;
                  }
                }
              }
            }
          });
        });
      });
    });

    return analysisData;
  };
// end helper functions

  /*
   *  Section Initial:
   *    Initial generation of stuff we need.
   *
   *  Builds:
   *    1.) Map: collectionNamesMap[id] -> Collection Name
   *    2.) Map: collectionReplicationFactorMap[name] -> Collection Replication Factor
   */
  buildCollectionNamesMap(); // builds id <-> name map && collection <-> replFactor map

  /*
   *  Section Analysis:
   *    Start first analysis object of current cluster shard distribution state.
   *    That object should not be modified afterwards
   *
   *  Builds:
   *    The result will be stored in the global variable: 'analysisData'
   */
  let analysisData = generateAnalysis(initAgencyCollections);

  /*
   *  Section Score:
   *    Is able to calculate a score for a 'analysisData' object.
   *
   *  Builds:
   *    The result will be stored in the global variable: 'analysisData'
   */
  let scores = [];
  scores.push(calculateCollectionsScore(analysisData));

  /*
   *  Section Extend Distribution Analysis:
   *    Extend information about master collections
   *
   *  Extends:
   *    shardBucketList: {
   *      masterCollection: {
   *        followers: <array>,
   *        replicationFactor: <number>,
   *        numberOfShards: <number>,
   *        totalAmountOfShards: <number> <-- NEW
   *      }
   *    }
   */
  extendShardBucketList();
  // print(shardBucketList);

  // print("=== Scores ===");
  // print(scores);

  /*
   *  Section Find Collection Candidates:
   *    Analyse the latest score and find possible move candidates
   *
   *  Builds:
   *    Populate collectionCandidates<collectionNames> array.
   */
  let candidates = getCandidatesToOptimize(scores[0]);
  // print("=== Potential candidates ===");
  // print(candidates);

  /*
   *  Section Optimized Iterations:
   *    Will fetch either:
   *      1.) First run, original agency state
   *      2.) Upcoming runs will take the last state from optimizedIterations
   *
   *   The idea is to move shards programmatically only and estimate distribution first.
   *
   *  Builds:
   *    The result of each iteration will be stored in optimizedIterations array
   */
  print("=== Optimizations ===");
  let optimizedIterations = []; // TODO: We should consider removing this for better performance
  optimizedIterations.push(moveShardsLocally(candidates, analysisData));
  scores.push(calculateCollectionsScore(analysisData));

  // the looping begins: top functions could join here as well, just wanted to keep
  // sections to better debug and comment things. can be changed later.

  for (var i = 0; i < MAX_ITERATIONS; i++) {
    print("Current iteration: " + i + " (+1)");
    candidates = getCandidatesToOptimize(scores[scores.length - 1]);
    optimizedIterations.push(moveShardsLocally(candidates, optimizedIterations[i]));
    scores.push(calculateCollectionsScore(optimizedIterations[i]));
  }

  // print("===== Final Score ===== ");
  // print(scores[scores.length - 1]);

  print("===== Summary ===== ");
  print("Actions done: " + jobHistory.length);
  print("Iterations Done: " + MAX_ITERATIONS + " (+1)");

  /*
   *  Section Optimize Plan:
   *    Remove possible duplicates out of the plan, if they occur!
   *
   *  Rewrites:
   *    Optimizes and changes jobHistory
   */
  // TODO: This needs to be implemented.

  /*
   *  Section Create Plan:
   *    Actually create the plan, if we have found a good result distribution.
   *
   *  Builds:
   *    Write moveShards plan to file.
   */
  // Save to file
  if (jobHistory.length > 0) {
    fs.write("moveShardsPlan.json", JSON.stringify(jobHistory));
  } else {
    print("No actions could be created. Exiting.")
  }

  /*
   *  DEBUG PRINTS (can be removed later)
   */
  print("=== Debug ===");
  print("Available DBServers: " + info.amountOfDatabaseServers);
  _.each(initAgencyCollections, function (collections) {
    _.each(collections, function (collection, cId) {
      //print(collection.name);
      if (collection.name === "TrendingThreats") {
        //  print("===========");
        // print(collection);
        //    print("Sharding after: " + collectionNamesMap[collection.distributeShardsLike]);
      }
    });
  });
  // print(scores);
  // print(shardBucketList);
  // print(Object.keys(initAgencyCollections));
  // print(jobHistory);
  // print(agencyDatabases);
  // print(analysisData);
};
