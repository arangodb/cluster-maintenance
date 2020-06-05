/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango, db */
exports.name = "create-move-analysis";
exports.group = "move shard tasks";
exports.args = [
  {"name": "dump-file", "optional": true, "type": "jsonfile", "description": "agency dump file"}
];
exports.args_arangosh = "| --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Creates analysis for a plan to rebalance shards in your cluster.";
exports.selfTests = ["arango", "db"];
exports.requires = "3.3.23 - 3.7.99";
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
  const dump = helper.getAgencyDumpFromObjectOrAgency(file)[0];
  const agencyPlan = dump.arango.Plan;
  const initAgencyCollections = agencyPlan.Collections;
  const health = dump.arango.Supervision.Health;

  // statics
  const MAX_ITERATIONS = 200;
  const debug = false;

  // Analysis Data Format
  // {
  //   databaseName: {
  //     collectionId: {
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
  let shardLeaderMoveHistory = [];
  let shardFollowerMoveHistory = [];
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
        collectionReplicationFactorMap[cId] = collection.replicationFactor;
      });
    });
  };

  let buildShardDistribution = function (collection, databaseName, distributeShardsLike) {
    let cObj = {};
    _.each(collection.shards, function (distribution, shardId) {
      cObj[shardId] = {};
      // cObj[shardId].nrFollowers = distribution.length;
      cObj[shardId].distribution = distribution;
      cObj[shardId].collectionName = collection.name;
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
    let distributeShardsLikeId = collection.distributeShardsLike;

    if (isInternalGraphsCollection(collection.name)) {
      // skipping internal graphs collections
      return;
    }

    if (!distributeShardsLikeId) {
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

    if (!shardBucketList[databaseName][distributeShardsLikeId]) {
      shardBucketList[databaseName][distributeShardsLikeId] = {
        followers: [],
        replicationFactor: collectionReplicationFactorMap[distributeShardsLikeId],
        numberOfShards: collection.numberOfShards,
        shardCollectionTotalAmount: collectionReplicationFactorMap[distributeShardsLikeId] * collection.numberOfShards
      };
    }
    if (!shardBucketList[databaseName][distributeShardsLikeId].followers.includes(collection.name)) {
      shardBucketList[databaseName][distributeShardsLikeId].followers.push(collection.name);
    }
  };

  // calculate some environment properties here
  let info = {
    amountOfDatabaseServers: getCountOfCurrentDatabaseServers(),
    dbServerNames: getNamesOfCurrentDatabaseServers()
  };

  // calculate helper methods
  let calculateShardDistributionInformation = function (totalShards, collectionId, leaders, followers) {
    let multipliedTotalShards = totalShards * collectionReplicationFactorMap[collectionId];
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

  let calulateCollectionScore = function (analysisData, collectionId, dbServerName, databaseNameToCheck) {
    // tries to calculate the distribution based on collection shards
    // TODO Review: Next step will also be to verify the distribution regarding leader <-> follower
    // No server should have leaders/followers only.
    // skip collections which do have distributeShardsLike (calculation differs)

    let score = -1;
    let leaders = 0;
    let followers = 0;
    let totalShards = 0;

    _.each(analysisData[databaseNameToCheck][collectionId], function (shard) {
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
      totalShards, collectionId, leaders, followers
    );

    let shardsWeHave = shardDistributeInfo.shardTotalAmount;
    if (shardsWeHave == shardDistributeInfo.perfectAmountOfShards && shardsWeHave !== 0) {
      // perfect distribution
      score = 1;
      //} else if (shardsWeHave >= shardDistributeInfo.lowerBound && shardsWeHave <= shardDistributeInfo.upperBound && shardsWeHave !== 0) {
      // we are in that range of lowerBound <-> upperBound, almost perfect distribution
      // score = 0.99;
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
        _.each(database, function (collection, collectionId) { // analysisData original
          let info = calulateCollectionScore(analysisData, collectionId, dbServerName, databaseName);
          let localScore = info[0];
          let distribution = info[1];

          // prepare empty objects
          if (!score[dbServerName][databaseName]) {
            score[dbServerName][databaseName] = {};
          }

          score[dbServerName][databaseName][collectionId] = {
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
              resultData[databaseName][collection.id] = {};
            }
            resultData[databaseName][collection.id] = lResult;
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

  let isBucketMaster = function (collectionId, databaseName) {
    if (shardBucketList[databaseName][collectionId]) {
      if (debug) {
        print("Collection: " + collectionId + " is a bucket master")
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
        _.each(collections, function (collection, collectionId) {
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
                collectionId: collectionId,
                database: databaseName
              });
            } else if (collection.distribution.shardFollowerAmount === 1) {
              singleShardCollectionDistribution[databaseServerName].followers.push({
                collectionId: collectionId,
                database: databaseName
              });
            }
            // we will not optimize it here related due to the score, as it makes no sense here
            if (debug) {
              // TODO: Check skipped ones in detail
              // print("Exiting : " + collectionId)
            }
            return;
          }

          if (!candidates[databaseName]) {
            candidates[databaseName] = {};
          }

          if (!candidates[databaseName][collectionId]) {
            // are we a bucket master?
            let bucketMaster = isBucketMaster(collectionId, databaseName);

            candidates[databaseName][collectionId] = {
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

          if (candidates[databaseName][collectionId].weakestScore === null || collection.score < candidates[databaseName][collectionId].weakestScore) {
            candidates[databaseName][collectionId].weakestScore = collection.score;
            candidates[databaseName][collectionId].leastFilledDatabaseServer = databaseServerName;
            candidates[databaseName][collectionId].weakestAmountOfLeaders = collection.distribution.shardLeaderAmount;
            candidates[databaseName][collectionId].weakestAmountOfFollowers = collection.distribution.shardFollowerAmount;
            candidates[databaseName][collectionId].perfectAmountOfShards = Math.floor(collection.distribution.perfectAmountOfShards);
            candidates[databaseName][collectionId].perfectAmountOfLeaders = Math.floor(collection.distribution.perfectAmountOfLeaders);
            candidates[databaseName][collectionId].perfectAmountOfFollowers = Math.floor(collection.distribution.perfectAmountOfFollowers);
          }

          if (candidates[databaseName][collectionId].bestScore === null || collection.score > candidates[databaseName][collectionId].bestScore) {
            candidates[databaseName][collectionId].bestScore = collection.score;
            candidates[databaseName][collectionId].mostFilledDatabaseServer = databaseServerName;
            candidates[databaseName][collectionId].bestAmountOfLeaders = collection.distribution.shardLeaderAmount;
            candidates[databaseName][collectionId].bestAmountOfFollowers = collection.distribution.shardFollowerAmount;
            candidates[databaseName][collectionId].perfectAmountOfShards = Math.floor(collection.distribution.perfectAmountOfShards);
            candidates[databaseName][collectionId].perfectAmountOfLeaders = Math.floor(collection.distribution.perfectAmountOfLeaders);
            candidates[databaseName][collectionId].perfectAmountOfFollowers = Math.floor(collection.distribution.perfectAmountOfFollowers);
          }

          candidates[databaseName][collectionId].scores.push({score: collection.score, db: databaseServerName});
        });
      });
    });

    return [candidates, checkSingleShardCollectionCandidates(singleShardCollectionDistribution)];
  };

  let calculateAmountOfCollectionShards = function (collectionId, database, withoutReplication) {
    // Single shard collection
    let amount = 1;
    if (isBucketMaster(collectionId, database)) {
      amount = shardBucketList[database][collectionId].followers.length + 1;
      if (!withoutReplication) {
        amount = amount * shardBucketList[database][collectionId].replicationFactor;
      }
    }
    return amount;
  };

  let calculateAmountOfDatabaseShards = function (collections) {
    // Single shard collections
    let amount = 0;
    _.each(collections, function (collection) {
      amount += calculateAmountOfCollectionShards(collection.collectionId, collection.database, true)
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
      result.distribution.totalAmountOfLeaders += calculateAmountOfDatabaseShards(dbServer.leaders);
      result.distribution.totalAmountOfFollowers += calculateAmountOfDatabaseShards(dbServer.followers);

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
      if (result.distribution.bestAmountOfFollowers === null || calculateAmountOfDatabaseShards(dbServer.followers) > result.distribution.bestAmountOfFollowers) {
        result.distribution.bestAmountOfFollowers = calculateAmountOfDatabaseShards(dbServer.followers);
        result.distribution.bestFollowerDatabaseServer = databaseServerName;
      }
    });

    result.distribution.perfectAmountOfLeaders = Math.floor(result.distribution.totalAmountOfLeaders / info.amountOfDatabaseServers);
    result.distribution.perfectAmountOfFollowers = Math.floor(result.distribution.totalAmountOfFollowers / info.amountOfDatabaseServers);

    return result;
  };

  let potentialOptimizations = false;
  let moveSingleShardLocally = function (shardId, fromDBServer, toDBServer,
                                         collectionId, isLeader, analysisData, databaseName) {
    // move shards in our local state only
    let success = false;

    /*
    if (isLeader) {
      if (shardLeaderMoveHistory.indexOf(shardId) !== -1) {
        print("already moved that leader shard.");
        // we already moved that shard and will not move it again
        return {
          success: success,
          data: null
        }
      }
    } else {
      if (shardFollowerMoveHistory.indexOf(shardId) !== -1) {
        print("already moved that follower shard.");
        // we already moved that shard and will not move it again
        return {
          success: success,
          data: null
        }
      }
    }
    */

    // TODO: re-enable upper logic, this needs some chnages in our agency!
    if (shardLeaderMoveHistory.indexOf(shardId) !== -1 || shardFollowerMoveHistory.indexOf(shardId) !== -1) {
      potentialOptimizations = true;
      return {
        success: success,
        data: null
      }
    }

    if (fromDBServer === toDBServer) {
      if (debug) {
        print("Best and worst server are equal. No actions needs to be done.");
      }
      // makes no sense to do this
      return {
        success: success,
        data: null
      }
    }

    // modifiy local state
    if (isLeader) {
      // remove old leader, add new one
      if (analysisData[databaseName][collectionId][shardId].distribution.indexOf(toDBServer) > 0) {
        // we are already follower, move not allowed
        if (debug) {
          print("This is not allowed to happen: Cannot move leader to a dbserver which has already a follower shard.");
        }
      } else {
        analysisData[databaseName][collectionId][shardId].distribution.shift();
        analysisData[databaseName][collectionId][shardId].distribution.unshift(toDBServer);
        success = true;
      }
    } else {
      // check that toDBServer is NOT a follower or a leader
      let toDBServerPos = analysisData[databaseName][collectionId][shardId].distribution.indexOf(toDBServer);
      if (toDBServerPos === -1) {
        // we are not a follower or a leader of this shard
        let fromDBServerPos = analysisData[databaseName][collectionId][shardId].distribution.indexOf(fromDBServer);
        if (fromDBServerPos === -1) {
          if (debug) {
            print("This is not allowed to happen - bad agency state.");
          }
        } else {
          analysisData[databaseName][collectionId][shardId].distribution[fromDBServerPos] = toDBServer;
          success = true;
        }
      } else {
        if (debug) {
          print("This is not allowed to happen - toDBServeris: " + toDBServer + " is either a follower or a leader already.");
        }
      }
    }

    if (success) {
      // store shardId in shardMoveHistory, to forbid further moves of the same shard
      if (isLeader) {
        shardLeaderMoveHistory.push(shardId);
      } else {
        shardFollowerMoveHistory.push(shardId);
      }

      // persist action history in local jobHistory
      jobHistory.push({
        // action: "moveShard",
        database: databaseName,
        collection: collectionId,
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

  // this function will move shards locally around and then return a new state
  let moveShardsLocally = function (candidates, analysisData) {
    // candidates[0]: are the "regular collections"
    // candidates[1]: are collections with a single shard

    let calcAmountToMove = function (bestAmount, weakestAmount, perfectAmount) {
      let amount = 0;

      if (bestAmount > weakestAmount) {
        if (bestAmount > perfectAmount) {
          // we need to move leaders
          let diff = bestAmount - perfectAmount;
          if (diff > perfectAmount) {
            // do not move too much (more then perfect) shards to the weakest db server
            amount = perfectAmount - weakestAmount;
          } else {
            amount = diff;
          }

          if (bestAmount - amount < perfectAmount) {
            amount = bestAmount - perfectAmount;
          }
        }
      }
      return amount;
    };

    // first detect the amount of what (leader/follower) to move
    _.each(candidates[0], function (database, databaseName) {
      _.each(database, function (stats, collectionId) {
        // calculate a regular collection
        let amountOfLeadersToMove = calcAmountToMove(stats.bestAmountOfLeaders, stats.weakestAmountOfLeaders, stats.perfectAmountOfLeaders);
        let amountOfFollowersToMove = calcAmountToMove(stats.bestAmountOfFollowers, stats.weakestAmountOfFollowers, stats.perfectAmountOfFollowers);

        if (amountOfLeadersToMove === 0 && amountOfFollowersToMove === 0) {
          // no change, quick exit: return same state
          return;
        }

        // now iterate through current state and start moving (local only!)
        for (let [databaseNameInner, databaseInner] of Object.entries(analysisData)) {
          if (databaseInner[collectionId] && databaseName === databaseNameInner) { // if collection got found inside that database
            for (let [shardId, shard] of Object.entries(databaseInner[collectionId])) {
              if (shard.distribution[0] === stats.mostFilledDatabaseServer) {
                // we found the best db server as leader for the current shard
                if (amountOfLeadersToMove > 0) {
                  let result = moveSingleShardLocally(
                    shardId, stats.mostFilledDatabaseServer, stats.leastFilledDatabaseServer,
                    collectionId, true, analysisData, databaseName
                  );
                  if (result.success) {
                    analysisData = result.data;
                    amountOfLeadersToMove--;
                  } else {
                    if (debug) {
                      print("Could not move leader shard.")
                    }
                  }
                }
              } else {
                // we might have a follower shard
                if (shard.distribution.indexOf(stats.mostFilledDatabaseServer) > 0) {
                  // we found dbserver as follower
                  if (amountOfFollowersToMove > 0) {
                    let result = moveSingleShardLocally(
                      shardId, stats.mostFilledDatabaseServer, stats.leastFilledDatabaseServer,
                      collectionId, false, analysisData, databaseName
                    );
                    if (result.success) {
                      analysisData = result.data;
                      amountOfFollowersToMove--;
                    } else {
                      if (debug) {
                        print("Could not move follower shard.")
                      }
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

    let moveHelper = function (bestAmount, perfectAmount, bestDatabaseServer, weakestDatabaseServer, leader) {
      if (bestAmount > perfectAmount) {
        let totalAmountToMove = bestAmount - perfectAmount;
	if (totalAmountToMove > perfectAmount) {
          totalAmountToMove = perfectAmount
        }

        let toIterate;
        if (leader) {
          toIterate = singleShardInfo[bestDatabaseServer].leaders;
        } else {
          toIterate = singleShardInfo[bestDatabaseServer].followers;
        }

        let amountAfterMove = totalAmountToMove;

        _.each(toIterate, function (collection) {
          let sAmount = calculateAmountOfCollectionShards(collection.collectionId, collection.database, true);

          if (amountAfterMove - sAmount >= 0) {
            // only if we do not drop below zero, we can to continue sum up shards to move
            let shardId = Object.keys(analysisData[collection.database][collection.collectionId])[0];
            let result = moveSingleShardLocally(
              shardId, bestDatabaseServer, weakestDatabaseServer,
              collection.collectionId, leader, analysisData, collection.database
            );
            if (result.success) {
              analysisData = result.data;
              amountAfterMove -= sAmount;
            } else {
              if (debug) {
                print("Not able to move. Rules in moveSingleShardLocally restrict it.");
              }
            }
          } else {
            if (debug) {
              print("We cannot move collection: " + collection.collectionId + ". Too much shards would be moved.");
            }
          }
        });
      }
    };

    // leaders
    moveHelper(
      singleShardDistribution.bestAmountOfLeaders,
      singleShardDistribution.perfectAmountOfLeaders,
      singleShardDistribution.bestLeaderDatabaseServer,
      singleShardDistribution.weakestLeaderDatabaseServer,
      true
    );

    // followers
    moveHelper(
      singleShardDistribution.bestAmountOfFollowers,
      singleShardDistribution.perfectAmountOfFollowers,
      singleShardDistribution.bestFollowerDatabaseServer,
      singleShardDistribution.weakestFollowerDatabaseServer,
      false
    );

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
    let spacing = "";
    if (value === 0) {
      spacing = "  ";
    } else if (value < 1) {
      spacing = " ";
    }

    if (disableColor) {
      return spacing + (Number.parseFloat(value * 100)).toFixed(2) + "%";
    }

    let SHELL_COLOR_RESET = "\x1b[0m";
    let SHELL_COLOR_GREEN = "\x1b[32m";
    let SHELL_COLOR_RED = "\x1b[31m";
    let SHELL_COLOR_YELLOW = "\x1b[33m";

    let selectedColor = SHELL_COLOR_YELLOW;
    if (value > 0.8 && value < 1.2) {
      selectedColor = SHELL_COLOR_GREEN;
    } else if (value < 0.5 || value > 1.5) {
      selectedColor = SHELL_COLOR_RED;
    }

    return spacing + selectedColor + (Number.parseFloat(value * 100)).toFixed(2) + "%" + SHELL_COLOR_RESET;
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
    let foundAtLeastAShardedCollection = false;
    let collectionStatistics = {
      optimized: 0,
      degraded: 0,
      equal: 0
    };

    // multiple shard description
    let shardedCollectionsTable = new AsciiTable('Scores - Sharded collections');
    let tableHeadings = [
      'Server',
      'Database',
      'Collection',
      'Score'
    ];
    shardedCollectionsTable.setHeading(tableHeadings);

    // single shard description
    let singleShardCollectionsTable = new AsciiTable('Scores - Single sharded collections');
    let singleShardTableHeadings = [
      'Database Server',
      'Shards',
      'Shards (new)',
      'Leaders',
      'Leaders (new)',
      'Follower',
      'Follower (new)',
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
        _.each(collections, function (collection, collectionId) {
          if (collection.distribution.singleShardCollection) {
            if (!amountOfSingleShardCollectionsPerDB[databaseServerName]) {
              amountOfSingleShardCollectionsPerDB[databaseServerName] = {
                start: 0,
                end: 0,
                leaderStart: 0,
                followerStart: 0,
                leaderEnd: 0,
                followerEnd: 0
              };
            }
            if (collection.distribution.shardTotalAmount === 1) {
              let amount = calculateAmountOfCollectionShards(collectionId, databaseName, true);
              if (collection.distribution.shardLeaderAmount === 1) {
                amountOfSingleShardCollectionsPerDB[databaseServerName].leaderStart += amount;
              } else {
                amountOfSingleShardCollectionsPerDB[databaseServerName].followerStart += amount;
              }
              amountOfSingleShardCollectionsPerDB[databaseServerName].start += amount;
              totalSingleShardCollections += amount;
            }
          } else {
            foundAtLeastAShardedCollection = true;
            shardedCollectionsTable.addRow([
              databaseServerName,
              databaseName,
              collectionId + "(" + collectionNamesMap[collectionId] + ")",
              scoreFormatter(
                collection.score) + " -> " + scoreFormatter(end[databaseServerName][databaseName][collectionId].score
              )
            ]);

            countScoreChange(
              collectionStatistics,
              collection.score,
              end[databaseServerName][databaseName][collectionId].score
            );
          }
        });
      });
    });

    _.each(end, function (database, databaseServerName) {
      _.each(database, function (collections, databaseName) {
        _.each(collections, function (collection, collectionId) {
          if (collection.distribution.singleShardCollection) {
            if (collection.distribution.shardTotalAmount === 1) {
              let amount = calculateAmountOfCollectionShards(collectionId, databaseName, true);
              if (collection.distribution.shardLeaderAmount === 1) {
                amountOfSingleShardCollectionsPerDB[databaseServerName].leaderEnd += amount;
              } else {
                amountOfSingleShardCollectionsPerDB[databaseServerName].followerEnd += amount;
              }
              amountOfSingleShardCollectionsPerDB[databaseServerName].end += amount;
            }
          }
        });
      });
    });

    if (foundAtLeastAShardedCollection) {
      if (collectionStatistics.optimized === 0 && collectionStatistics.degraded === 0) {
        print();
        print("No possibilities to optimize sharded (numberOfShards > 1) collections.");
      } else {
        print();
        print(shardedCollectionsTable.toString());
        printScoreChange(collectionStatistics);
        print();
      }
    }

    if (Object.keys(amountOfSingleShardCollectionsPerDB).length > 0) {
      let bestDistribution = Math.round(totalSingleShardCollections / info.dbServerNames.length);
      _.each(amountOfSingleShardCollectionsPerDB, function (databaseServer, databaseServerName) {
        let scoreEnd = 0;
        let scoreStart = 0;

        if (databaseServer.start !== 0) {
           scoreStart = databaseServer.start / bestDistribution;
        }

        if (databaseServer.end !== 0) {
          scoreEnd = databaseServer.end / bestDistribution;
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
          databaseServer.leaderStart,
          databaseServer.leaderEnd,
          databaseServer.followerStart,
          databaseServer.followerEnd,
          scoreFormatter(scoreStart) + " -> " + scoreFormatter(scoreEnd)
        ]);
      });

      if (singleShardCollectionStatistics.optimized === 0 && singleShardCollectionStatistics.degraded === 0) {
        print("No possibilities to optimize single sharded (numberOfShards = 1) collections.");
      } else {
        print("");
        print(singleShardCollectionsTable.toString());
        printScoreChange(singleShardCollectionStatistics);
      }
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
  print("Trying to optimize ...");
  let optimizedIterations = []; // TODO: We should consider removing this for better performance
  optimizedIterations.push(moveShardsLocally(candidates[candidates.length - 1], analysisData));
  scores.push(calculateCollectionsScore(analysisData));

  // the looping begins: top functions could join here as well, just wanted to keep
  // sections to better debug and comment things. can be changed later.

  let oldJobHistoryLenght = jobHistory.length;
  for (var i = 0; i < MAX_ITERATIONS; i++) {
    if (debug) {
      print("Iteration: " + (i + 1) + " started.");
    }
    candidates.push(getCandidatesToOptimize(scores[scores.length - 1]));
    optimizedIterations.push(moveShardsLocally(candidates[candidates.length - 1], optimizedIterations[optimizedIterations.length - 1]));
    scores.push(calculateCollectionsScore(optimizedIterations[candidates.length - 1]));

    if (oldJobHistoryLenght === jobHistory.length && jobHistory.length !== 0) {
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

    print();
    print("=== Info ===");
    if (potentialOptimizations) {
      print();
      print("There are optimizations which could not been handled in this run. After");
      print("all started operations are done, feel free to re-use that script again.");
      print("This will lead to a better overall distribution, if you're not satisfied");
      print("with the current scores yet.");
    }
    print();
    print("Use \"execute-move-plan\" to execute the created \"moveShardsPlan.json\"");
    print("  -> Use a coordinator endpoint");
    print();
    print("Use \"show-move-shards\" to track the current progress of your move shard");
    print("jobs. If there are no operations left, you can continue with the next");
    print("optimization iteration.");
    print("  -> Use the agency leader endpoint");
  } else {
    print("No optimizations are available. Exiting.")
  }

  print("");
};
