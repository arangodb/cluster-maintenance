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

Execute the analyze shard script (will also create a move plan "moveShardsPlan.json"):
 - arangosh --javascript.execute ../debug-scripts/debugging/index.js  create-move-analysis --server.endpoint tcp://(ip-address):(agency-port)> (agency)
 - arangosh --javascript.execute ../debug-scripts/debugging/index.js  create-move-analysis --server.endpoint agencyDump.json (dump)
`;

const AsciiTable = require('../3rdParty/ascii-table');

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
  const MIN_ALLOWED_SCORE = 0.95;
  const MAX_ITERATIONS = 5;
  const debug = false;

  // Analysis Data Format
  // {
  //   databaseName: {
  //     collectionName: {
  //       shardName: {
  //         distribution: [
  //           'databaseServerLeader-ID',
  //           'databaseServerFollowerA-ID',
  //           'databaseServerFollowerB-ID',
  //           'databaseServerFollowerC-ID',
  //            ...
  //         ]
  //       },
  //       ..
  //     },
  //     leaderCollectionB: {},
  //      ...
  //   }
  //  ...
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

  let isInternalGraphsCollection = function (collectionName) {
    if (collectionName.charAt(0) === '_') {
      if (collectionName.substring(0, 3) === '_to_') {
        return false;
      } else if (collectionName.substring(0, 6) === '_from_') {
        return false;
      } else if (collectionName.substring(0, 7) === '_local_') {
        return false;
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
    if (isInternalGraphsCollection(collection.name)) {
      // skipping internal graphs collections
      return;
    }

    let sharding = buildShardDistribution(collection, databaseName);
    if (Object.keys(sharding).length > 0) {
      return sharding;
    } else {
      if (debug) {
        print("Debug: Empty sharding for collection " + collection.name)
      }
      return;
    }
  };

  // a "follower collection" has distributeShardsLike, will be maintained in buckets
  let addFollowerCollection = function (collection, databaseName) {
    let distributeShardsLike = collectionNamesMap[collection.distributeShardsLike];

    if (isInternalGraphsCollection(collection.name)) {
      // skipping internal graphs collections
      return;
    }

    if (!distributeShardsLike) {
      // invalid state
      return;
    }

    if (collection.numberOfShards === 0) {
      if (debug) {
        let msg = "Debug - Skipped collection: " + collection.name;
        if (collection.shadowCollections && collection.shadowCollections.length > 0) {
          msg += " - Reason: Shadow collection";
        } else {
          msg += "- Reason: Unknown.";
        }
        print(msg);
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
    let shardTotalAmount = leaders + followers;

    let singleShardCollection = false;
    if (totalShards === 1) {
      // we have a single shard collection, mark it as one.
      singleShardCollection = true;
    }

    return {
      perfectAmountOfShards: perfectAmountOfShards,
      perfectAmountOfLeaders: perfectAmountOfLeaders,
      perfectAmountOfFollowers: perfectAmountOfShards - perfectAmountOfLeaders,
      upperBound: Math.ceil(multipliedTotalShards / info.amountOfDatabaseServers),
      lowerBound: Math.floor(multipliedTotalShards / info.amountOfDatabaseServers),
      shardLeaderAmount: leaders,
      shardFollowerAmount: followers,
      shardTotalAmount: shardTotalAmount,
      singleShardCollection: singleShardCollection
    }
  };

  let calulateCollectionScore = function (analysisData, collectionName, dbServerName, databaseNameToCheck) {
    // tries to calculate the distribution based on collection shards
    // TODO Review: Next step will also be to verify the distribution regarding leader <-> follower
    // No server should have leaders/followers only.
    // skip collections which do have distributeShardsLike (calculation differs)

    let score = -1;
    let leaders = 0;
    let followers = 0;
    let totalShards = 0;

    _.each(analysisData[databaseNameToCheck][collectionName], function (shard) {
      if (shard.distribution[0] === dbServerName) {
        leaders++;
      } else {
        if (shard.distribution.indexOf(dbServerName) > 0) {
          followers++;
        }
      }
      totalShards++;
    });

    let shardDistributeInfo = calculateShardDistributionInformation(
      totalShards, collectionName, leaders, followers
    );

    let shardsWeHave = shardDistributeInfo.shardTotalAmount;
    if (shardsWeHave == shardDistributeInfo.perfectAmountOfShards && shardsWeHave !== 0) {
      // perfect distribution
      score = 1;
    } else if (shardsWeHave >= shardDistributeInfo.lowerBound && shardsWeHave <= shardDistributeInfo.upperBound && shardsWeHave !== 0) {
      // we are in that range of lowerBound <-> upperBound, almost perfect distribution
      score = 0.99;
    } else if (shardsWeHave > shardDistributeInfo.perfectAmountOfShards) {
      score = shardsWeHave / shardDistributeInfo.perfectAmountOfShards;
    } else if (shardsWeHave < shardDistributeInfo.perfectAmountOfShards && shardsWeHave !== 0) {
      // we have less then perfect shards, we might need fill that one up
      score = shardsWeHave / shardDistributeInfo.perfectAmountOfShards;
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
          let info = calulateCollectionScore(analysisData, collectionName, dbServerName, databaseName);
          let localScore = info[0];
          let distribution = info[1];

          // prepare empty objects
          if (!score[dbServerName][databaseName]) {
            score[dbServerName][databaseName] = {};
          }

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

        if (isInternalGraphsCollection(collection.name)) {
          if (debug) {
            print("SKIPPED: " + collection.name);
          }
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

  let isBucketMaster = function (collectionName, databaseName) {
    if (shardBucketList[databaseName][collectionName]) {
      if (debug) {
        print("Collection: " + collectionName + " is a bucket master")
      }
      return true;
    } else {
      return false;
    }
  };

  let getCandidatesToOptimize = function (score) {
    /*
     * {
     *   worstDatabaseServer: abc,
     *   mostFilledDatabaseServer: xyz,
     * }
     */

    let singleShardCollectionDistribution = {};
    let candidates = {};

    _.each(score, function (database, databaseServerName) {
      _.each(database, function (collections, databaseName) {
        _.each(collections, function (collection, collectionName) {
          if (collection.distribution.singleShardCollection) {
            // if we found a single shard collection, keep and count the amount of single shard collections per server
            if (!singleShardCollectionDistribution[databaseServerName]) {
              singleShardCollectionDistribution[databaseServerName] = {
                leaders: [],
                followers: []
              };
            }
            if (collection.distribution.shardLeaderAmount === 1) {
              singleShardCollectionDistribution[databaseServerName].leaders.push({
                collection: collectionName,
                database: databaseName
              });
            } else if (collection.distribution.shardFollowerAmount === 1) {
              singleShardCollectionDistribution[databaseServerName].followers.push({
                collection: collectionName,
                database: databaseName
              });
            }
            // we will not optimize it here related due to the score, as it makes no sense here
            if (debug) {
              // TODO: Check skipped ones in detail
              // print("Exiting : " + collectionName)
            }
            return;
          }

          if (!candidates[databaseName]) {
            candidates[databaseName] = {};
          }

          if (!candidates[databaseName][collectionName]) {
            // are we a bucket master?
            let bucketMaster = isBucketMaster(collectionName, databaseName);

            candidates[databaseName][collectionName] = {
              bestScore: null,
              bestAmountOfLeaders: null,
              bestAmountOfFollowers: null,
              mostFilledDatabaseServer: null,
              perfectAmountOfShards: null,
              scores: [],
              weakestScore: null,
              weakestAmountOfLeaders: null,
              weakestAmountOfFollowers: null,
              leastFilledDatabaseServer: null,
              isBucketMaster: bucketMaster
            };
          }

          if (candidates[databaseName][collectionName].weakestScore === null || collection.score < candidates[databaseName][collectionName].weakestScore) {
            candidates[databaseName][collectionName].weakestScore = collection.score;
            candidates[databaseName][collectionName].leastFilledDatabaseServer = databaseServerName;
            candidates[databaseName][collectionName].weakestAmountOfLeaders = collection.distribution.shardLeaderAmount;
            candidates[databaseName][collectionName].weakestAmountOfFollowers = collection.distribution.shardFollowerAmount;
            candidates[databaseName][collectionName].perfectAmountOfShards = collection.distribution.perfectAmountOfShards;
            candidates[databaseName][collectionName].perfectAmountOfLeaders = collection.distribution.perfectAmountOfLeaders;
            candidates[databaseName][collectionName].perfectAmountOfFollowers = collection.distribution.perfectAmountOfFollowers;
          }

          if (candidates[databaseName][collectionName].bestScore === null || collection.score > candidates[databaseName][collectionName].bestScore) {
            candidates[databaseName][collectionName].bestScore = collection.score;
            candidates[databaseName][collectionName].mostFilledDatabaseServer = databaseServerName;
            candidates[databaseName][collectionName].bestAmountOfLeaders = collection.distribution.shardLeaderAmount;
            candidates[databaseName][collectionName].bestAmountOfFollowers = collection.distribution.shardFollowerAmount;
            candidates[databaseName][collectionName].perfectAmountOfShards = collection.distribution.perfectAmountOfShards;
            candidates[databaseName][collectionName].perfectAmountOfLeaders = collection.distribution.perfectAmountOfLeaders;
            candidates[databaseName][collectionName].perfectAmountOfFollowers = collection.distribution.perfectAmountOfFollowers;
          }

          candidates[databaseName][collectionName].scores.push(collection.score);
        });
      });
    });

    return [sortCandidates(candidates), checkSingleShardCollectionCandidates(singleShardCollectionDistribution)];
  };

  let sortCandidates = function (candidates) {
    // TODO: needs to be implemented (bucket first)
    // print(candidates);
    return candidates;
  };

  let calculateAmountOfCollectionShards = function (collection, database, withoutReplication) {
    // Single shard collection
    let amount = 1;
    if (isBucketMaster(collection, database)) {
      amount = shardBucketList[database][collection].followers.length;
      if (!withoutReplication) {
        amount = amount * shardBucketList[database][collection].replicationFactor;
      }
    }
    return amount;
  };

  let calculateAmountOfDatabaseShards = function (collections) {
    // Single shard collections
    let amount = 0;
    _.each(collections, function (collection) {
      amount += calculateAmountOfCollectionShards(collection.collection, collection.database, false)
    });
    return amount;
  };

  let checkSingleShardCollectionCandidates = function (singleShardCollectionDistribution) {
    let result = {
      info: singleShardCollectionDistribution,
      distribution: {
        perfectAmountOfLeaders: null,
        perfectAmountOfFollowers: null,
        bestAmountOfLeaders: null,
        bestAmountOfFollowers: null,
        bestLeaderDatabaseServer: null,
        bestFollowerDatabaseServer: null,
        weakestAmountOfLeaders: null,
        weakestAmountOfFollowers: null,
        weakestLeaderDatabaseServer: null,
        weakestFollowerDatabaseServer: null,
        totalAmountOfLeaders: 0,
        totalAmountOfFollowers: 0
      }
    };

    _.each(singleShardCollectionDistribution, function (dbServer, databaseServerName) {

      _.each(dbServer.leaders, function (cEntity) {
        result.distribution.totalAmountOfLeaders += calculateAmountOfCollectionShards(cEntity.collection, cEntity.database);
      });
      _.each(dbServer.followers, function (cEntity) {
        result.distribution.totalAmountOfFollowers += calculateAmountOfCollectionShards(cEntity.collection, cEntity.database);
      });

      if (result.distribution.weakestAmountOfLeaders === null || calculateAmountOfDatabaseShards(dbServer.leaders) < result.distribution.weakestAmountOfLeaders) {
        result.distribution.weakestAmountOfLeaders = calculateAmountOfDatabaseShards(dbServer.leaders);
        result.distribution.weakestLeaderDatabaseServer = databaseServerName;
      }
      if (result.distribution.weakestAmountOfFollowers === null || calculateAmountOfDatabaseShards(dbServer.followers) < result.distribution.weakestAmountOfFollowers) {
        result.distribution.weakestAmountOfFollowers = calculateAmountOfDatabaseShards(dbServer.followers);
        result.distribution.weakestFollowerDatabaseServer = databaseServerName;
      }
      if (result.distribution.bestAmountOfLeaders === null || calculateAmountOfDatabaseShards(dbServer.leaders) > result.distribution.bestAmountOfLeaders) {
        result.distribution.bestAmountOfLeaders = calculateAmountOfDatabaseShards(dbServer.leaders);
        result.distribution.bestLeaderDatabaseServer = databaseServerName;
      }
      if (result.distribution.bestAmountOfFollowers === null || calculateAmountOfDatabaseShards(dbServer.followers) < result.distribution.bestAmountOfFollowers) {
        result.distribution.bestAmountOfFollowers = calculateAmountOfDatabaseShards(dbServer.followers);
        result.distribution.bestFollowerDatabaseServer = databaseServerName;
      }
    });

    result.distribution.perfectAmountOfLeaders = Math.round(result.distribution.totalAmountOfLeaders / info.amountOfDatabaseServers);
    result.distribution.perfectAmountOfFollowers = Math.round(result.distribution.totalAmountOfFollowers / info.amountOfDatabaseServers);

    return result;
  };

  let moveSingleShardLocally = function (shardId, fromDBServer, toDBServer,
                                         collectionName, isLeader, analysisData, databaseName) {
    // move shards in our local state only
    // debug:
    // print("Moving: " + shardId + " from: " + fromDBServer + " to: " + toDBServer + "(leader: " + isLeader + ")");

    let success = false;

    if (fromDBServer == toDBServer) {
      // print("Best and worst server are equal. No actions needs to be done.")
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
        // print("THIS IS NOT ALLOWED TO HAPPEN")
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

  let initialAmountOfShardsPerDBServer = null;
  let modificatedAmountOfShardsPerDBServer = {};

  let helperTotalAmountOfShards = function () {
    if (!initialAmountOfShardsPerDBServer) {
      helper.extractDatabases(initialAmountOfShardsPerDBServer, dump);
      helper.extractPrimaries(initialAmountOfShardsPerDBServer, dump);
    }
  };

  let getTotalAmountOfShards = function (databaseServer) {
    if (!initialAmountOfShardsPerDBServer) {
      // calculate only once
      helperTotalAmountOfShards();

      _.each(info.dbServerNames, function (dbs) {
        let shards = initialAmountOfShardsPerDBServer.shardsPrimary[dbs];
        modificatedAmountOfShardsPerDBServer[dbserver] = shards.leaders.length + shards.followers.length;
      });
    }

    return modificatedAmountOfShardsPerDBServer[databaseServer];
  };

  // this function will move shards locally around and then return a new state
  let moveShardsLocally = function (candidates, analysisData) {
    print(" == START MOVE JOB ==")
    // candidates[0]: are the "regular collections"
    // candidates[1]: are collections with a single shard

    // TODO: Currently it is random if a "bucket" or a "single shard" is moved
    // TODO: We should first move buckets around, then single shards!

    // first detect the amount of what (leader/follower) to move
    _.each(candidates[0], function (database, databaseName) {
      _.each(database, function (stats, collectionName) {
        let amountOfLeadersToMove = 0;
        let amountOfFollowersToMove = 0;

        // special condition:
        // if we are a masterBucket collection, we need to take a look at the global
        // shard distribution per database before we start moving.
        if (false) {
          print("Bucket name: " + collectionName)
          let amountOfTotalShardsOfMostFilledDatabaseServer = getTotalAmountOfShards(
            stats.mostFilledDatabaseServer
          );
          let amountOfTotalShardsOfLeastFilledDatabaseServer = getTotalAmountOfShards(
            stats.leastFilledDatabaseServer
          );

          print("============================================================================")
          print("Found a bucket master: " + collectionName);
          print("Database: " + databaseName);
          print("Total amount of best: " + amountOfTotalShardsOfMostFilledDatabaseServer);
          print("Total amount of worst: " + amountOfTotalShardsOfLeastFilledDatabaseServer);
          print("============================================================================")


          if (amountOfTotalShardsOfMostFilledDatabaseServer > amountOfTotalShardsOfLeastFilledDatabaseServer) {
            let shardDifference = amountOfTotalShardsOfMostFilledDatabaseServer - amountOfTotalShardsOfLeastFilledDatabaseServer;
            if (shardDifference > shardBucketList[databaseName][collectionName].followers.length + 1) {
              // TODO: Move bucket calculation - Check if we could calculate more precise
              amountOfLeadersToMove = 1;
              amountOfFollowersToMove = 1;
              print("We will move a bucket");
            } else {
              // no change - we are not moving the bucket, quick exit: return same state
              print("We will not move a bucket 1");
              return;
            }
          } else {
            print("We will not move a bucket 2");
            // no change - we are not moving the bucket, quick exit: return same state
            return;
          }
        } else {
          // calculate a regular collection
          // TODO Review: Do not move more then perfect amount to a weak candidate
          if (stats.bestAmountOfLeaders > stats.weakestAmountOfLeaders) {
            if (stats.bestAmountOfLeaders > stats.perfectAmountOfLeaders) {
              // we need to move leaders
              let diff = stats.bestAmountOfLeaders - stats.perfectAmountOfLeaders;
              if (diff > stats.perfectAmountOfLeaders) {
                // do not move too much (more then perfect) shards to the weakest db server
                amountOfLeadersToMove = stats.perfectAmountOfLeaders - stats.weakestAmountOfLeaders;
              } else {
                amountOfLeadersToMove = diff;
              }

              if (stats.bestAmountOfLeaders - amountOfLeadersToMove < stats.perfectAmountOfLeaders) {
                amountOfLeadersToMove = stats.bestAmountOfLeaders - stats.perfectAmountOfLeaders;
              }
            }
          }
          if (stats.bestAmountOfFollowers > stats.weakestAmountOfFollowers) {
            if (stats.bestAmountOfFollowers > stats.perfectAmountOfFollowers) {
              // we need to move followers
              let diff = stats.bestAmountOfFollowers - stats.perfectAmountOfFollowers;
              if (diff > stats.perfectAmountOfFollowers) {
                // do not move too much (more then perfect) shards to the weakest db server
                amountOfFollowersToMove = stats.perfectAmountOfFollowers - stats.weakestAmountOfFollowers;
              } else {
                amountOfFollowersToMove = diff;
              }

              if (stats.bestAmountOfFollowers - amountOfFollowersToMove < stats.perfectAmountOfFollowers) {
                amountOfFollowersToMove = stats.bestAmountOfFollowers - stats.perfectAmountOfFollowers;
              }
            }
          }

          print("Leaders to move: " + amountOfLeadersToMove);
          print("Followers to move: " + amountOfFollowersToMove);

          if (amountOfFollowersToMove === 0 && amountOfLeadersToMove === 0) {
            // no change, quick exit: return same state
            return;
          }
        }

        // now iterate through current state and start moving (local only!)
        for (let [databaseName, database] of Object.entries(analysisData)) {
          if (database[collectionName]) { // if collection got found inside that database // TODO: @michael verify pls if correct
            for (let [shardId, shard] of Object.entries(database[collectionName])) {
              if (shard.distribution[0] === stats.mostFilledDatabaseServer) {
                // we found the best db server as leader for the current shard
                if (amountOfLeadersToMove > 0) {
                  let result = moveSingleShardLocally(
                    shardId, stats.mostFilledDatabaseServer, stats.leastFilledDatabaseServer,
                    collectionName, true, analysisData, databaseName
                  );
                  if (result.success) {
                    analysisData = result.data;
                    modificatedAmountOfShardsPerDBServer[stats.leastFilledDatabaseServer]++;
                    modificatedAmountOfShardsPerDBServer[stats.mostFilledDatabaseServer]--;
                    amountOfLeadersToMove--;
                  }
                }
              } else {
                // we might have a follower shard
                if (shard.distribution.indexOf(stats.mostFilledDatabaseServer) > 0) {
                  // we found dbserver as follower
                  if (amountOfFollowersToMove > 0) {
                    let result = moveSingleShardLocally(
                      shardId, stats.mostFilledDatabaseServer, stats.leastFilledDatabaseServer,
                      collectionName, false, analysisData, databaseName
                    );
                    if (result.success) {
                      analysisData = result.data;
                      modificatedAmountOfShardsPerDBServer[stats.leastFilledDatabaseServer]++;
                      modificatedAmountOfShardsPerDBServer[stats.mostFilledDatabaseServer]--;
                      amountOfFollowersToMove--;
                    }
                  }
                }
              }

              if (amountOfFollowersToMove === 0 && amountOfLeadersToMove === 0) {
                break;
              }
            }
          }
        }
      });
    });

    // now move single sharded collections as well
    let singleShardInfo = candidates[1].info;
    let singleShardDistribution = candidates[1].distribution;

    // leaders
    if (singleShardDistribution.bestAmountOfLeaders > singleShardDistribution.perfectAmountOfLeaders) {
      let amountOfLeadersToMove = singleShardDistribution.bestAmountOfLeaders - singleShardDistribution.perfectAmountOfLeaders;

      let collectionsToBeMoved = [];
      _.each(singleShardInfo[singleShardDistribution.bestLeaderDatabaseServer].leaders, function (leader) {
        let sAmount = calculateAmountOfCollectionShards(leader.collection, leader.database, true);

        if (amountOfLeadersToMove - sAmount >= 0 && amountOfLeadersToMove - sAmount >= singleShardDistribution.perfectAmountOfLeaders) {
          collectionsToBeMoved.push(leader);
          amountOfLeadersToMove -= sAmount;
        }
      });

      _.each(collectionsToBeMoved, function (cEntity) {
        let shardId = Object.keys(analysisData[cEntity.database][cEntity.collection])[0];
        let result = moveSingleShardLocally(
          shardId, singleShardDistribution.bestLeaderDatabaseServer, singleShardDistribution.weakestLeaderDatabaseServer,
          cEntity.collection, true, analysisData, cEntity.database
        );
        if (result.success) {
          analysisData = result.data;
        }
      });
    }

    // followers
    if (singleShardDistribution.bestAmountOfFollowers > singleShardDistribution.perfectAmountOfFollowers) {
      let amountOfFollowersToMove = singleShardDistribution.bestAmountOfFollowers - singleShardDistribution.perfectAmountOfFollowers;
      let collectionsToBeMoved = singleShardDistribution.bestAmountOfFollowers - singleShardDistribution.perfectAmountOfFollowers;

      _.each(singleShardInfo[singleShardDistribution.bestFollowerDatabaseServer].followers, function (follower) {
        let sAmount = calculateAmountOfCollectionShards(follower.collection, follower.database, true);

        if (amountOfFollowersToMove - sAmount >= 0 && amountOfFollowersToMove - sAmount >= singleShardDistribution.perfectAmountOfFollowers) {
          collectionsToBeMoved.push(follower);
          amountOfFollowersToMove -= sAmount;
        }
      });

      _.each(collectionsToBeMoved, function (cEntity) {
        let shardId = Object.keys(analysisData[cEntity.database][cEntity.collection])[0];
        let result = moveSingleShardLocally(
          shardId, singleShardDistribution.bestFollowerDatabaseServer, singleShardDistribution.weakestFollowerDatabaseServer,
          cEntity.collection, false, analysisData, cEntity.database // TODO: check isLeader flag
        );
        if (result.success) {
          analysisData = result.data;
        }
      });
    }

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

  let scoreFormatter = function (value, disableColor) {
    if (disableColor) {
      return Number.parseFloat(value).toFixed(4);
    }

    let SHELL_COLOR_RESET = "\x1b[0m";
    let SHELL_COLOR_GREEN = "\x1b[32m";
    let SHELL_COLOR_RED = "\x1b[31m";
    let SHELL_COLOR_YELLOW = "\x1b[33m";

    let selectedColor = SHELL_COLOR_YELLOW;
    if (value > 0.75) {
      selectedColor = SHELL_COLOR_GREEN;
    } else if (value < 0.25) {
      selectedColor = SHELL_COLOR_RED;
    }

    return selectedColor + Number.parseFloat(value).toFixed(4) + SHELL_COLOR_RESET;
  };

  let printScoreComparison = function (scores) {
    let countScoreChange = function (object, start, end) {
      if (Math.abs(end - 1) < Math.abs(start - 1)) {
        object.optimized++;
      } else if (Math.abs(end - 1) > Math.abs(start - 1)) {
        object.degraded++;
      } else {
        object.equal++;
      }
    };

    let printScoreChange = function (object) {
      var scoreTable = new AsciiTable('Score changes: ');
      let scoreHeadings = [
        'Optimized',
        'Degraded',
        'Unchanged'
      ];
      scoreTable.setHeading(scoreHeadings);
      scoreTable.addRow([
        object.optimized, object.degraded, object.equal
      ]);
      print(scoreTable.toString());
    };

    let start = scores[0];
    let end = scores[scores.length - 1];
    let amountOfSingleShardCollectionsPerDB = {};
    let foundAtLeastOneShardedCollection = false;
    let collectionStatistics = {
      optimized: 0,
      degraded: 0,
      equal: 0
    };

    // multiple shard description
    var shardedCollectionsTable = new AsciiTable('Scores - Sharded collections');
    let tableHeadings = [
      'Server',
      'Database',
      'Collection',
      'Score'
    ];
    shardedCollectionsTable.setHeading(tableHeadings);

    // single shard description
    var singleShardCollectionsTable = new AsciiTable('Scores - Single sharded collections');
    let singleShardTableHeadings = [
      'Database Server',
      'Amount (old)',
      'Amount (new)',
      'Score'
    ];
    singleShardCollectionsTable.setHeading(singleShardTableHeadings);
    let totalSingleShardCollections = 0;
    let singleShardCollectionStatistics = {
      optimized: 0,
      degraded: 0,
      equal: 0
    };

    _.each(start, function (database, databaseServerName) {
      _.each(database, function (collections, databaseName) {
        _.each(collections, function (collection, collectionName) {
          if (collection.distribution.singleShardCollection) {
            if (!amountOfSingleShardCollectionsPerDB[databaseServerName]) {
              amountOfSingleShardCollectionsPerDB[databaseServerName] = {
                start: 0,
                end: 0
              };
            }
            if (collection.distribution.shardTotalAmount === 1) {
              let amount = calculateAmountOfCollectionShards(collectionName, databaseName, true);
              amountOfSingleShardCollectionsPerDB[databaseServerName].start += amount;
              totalSingleShardCollections += amount;
            }
          } else {
            foundAtLeastOneShardedCollection = true;
            shardedCollectionsTable.addRow([
              databaseServerName,
              databaseName,
              collectionName,
              scoreFormatter(
                collection.score) + " -> " + scoreFormatter(end[databaseServerName][databaseName][collectionName].score
              )
            ]);

            countScoreChange(
              collectionStatistics,
              collection.score,
              end[databaseServerName][databaseName][collectionName].score
            );
          }
        });
      });
    });

    _.each(end, function (database, databaseServerName) {
      _.each(database, function (collections, databaseName) {
        _.each(collections, function (collection, collectionName) {
          if (collection.distribution.singleShardCollection) {
            if (collection.distribution.shardTotalAmount === 1) {
              let amount = calculateAmountOfCollectionShards(collectionName, databaseName, true);
              amountOfSingleShardCollectionsPerDB[databaseServerName].end += amount;
            }
          }
        });
      });
    });

    if (foundAtLeastOneShardedCollection) {
      print("");
      print(shardedCollectionsTable.toString());
      printScoreChange(collectionStatistics);
      print("");
    }
    if (Object.keys(amountOfSingleShardCollectionsPerDB).length > 0) {
      let bestDistribution = Math.round(totalSingleShardCollections / info.dbServerNames.length);
      _.each(amountOfSingleShardCollectionsPerDB, function (databaseServer, databaseServerName) {
        let scoreEnd = 0;
        let scoreStart = 0;

        if (databaseServer.start !== 0) {
          if (databaseServer.start < bestDistribution) {
            scoreStart = databaseServer.start / bestDistribution;
          } else {
            scoreStart = bestDistribution / databaseServer.start;
          }
        }

        if (databaseServer.end !== 0) {
          if (databaseServer.end > bestDistribution) {
            scoreEnd = bestDistribution / databaseServer.end;
          } else {
            scoreEnd = databaseServer.end / bestDistribution;
          }
        }


        countScoreChange(
          singleShardCollectionStatistics,
          scoreStart,
          scoreEnd
        );
        singleShardCollectionsTable.addRow([
          databaseServerName,
          databaseServer.start,
          databaseServer.end,
          scoreFormatter(scoreStart) + " -> " + scoreFormatter(scoreEnd)
        ]);
      });
      print("");
      print(singleShardCollectionsTable.toString());
      printScoreChange(singleShardCollectionStatistics);
      print("");
    }
  };

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
  let candidates = [];
  candidates.push(getCandidatesToOptimize(scores[0]));
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
  print("");
  print("===== Optimizations =====");
  print("");
  let optimizedIterations = []; // TODO: We should consider removing this for better performance
  optimizedIterations.push(moveShardsLocally(candidates[candidates.length - 1], analysisData));
  scores.push(calculateCollectionsScore(analysisData));

  // the looping begins: top functions could join here as well, just wanted to keep
  // sections to better debug and comment things. can be changed later.

  let oldJobHistoryLenght = jobHistory.length;
  for (var i = 0; i < MAX_ITERATIONS; i++) {
    candidates.push(getCandidatesToOptimize(scores[scores.length - 1]));
    optimizedIterations.push(moveShardsLocally(candidates[candidates.length - 1], optimizedIterations[i]));
    scores.push(calculateCollectionsScore(optimizedIterations[i]));

    if (oldJobHistoryLenght == jobHistory.length) {
      // we did not find any new possible optimizations.
      print("Done. No more optimizations could be added.")
      break;
    }
    oldJobHistoryLenght = jobHistory.length;
  }

  print("");
  print("===== Results ===== ");
  printScoreComparison(scores);

  print("===== Summary ===== ");
  print("");
  print("Potential actions found in total: " + jobHistory.length);

  /*
   *  Section Optimize Plan:
   *    Remove possible duplicates out of the plan, if they occur!
   *
   *  Rewrites:
   *    Optimizes and changes jobHistory
   */
  // TODO: This needs to be implemented.
  // TODO: Review: Prevent movement loops between dbservers

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
    print("Written to file: \"moveShardsPlan.json\"");
  } else {
    print("No actions could be created. Exiting.")
  }

  /*
   *  DEBUG PRINTS (can be removed later)
   */
  if (debug) {
    print("");
    print("=== Debug ===");
    print("");
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
    // print(scores[scores.length - 1])
  }

  print("");
  // print(shardBucketList)
};
