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
  const agencyDatabases = agencyPlan.Databases;
  const initAgencyCollections = agencyPlan.Collections;
  const health = dump.arango.Supervision.Health;

  // statics
  const MIN_ALLOWED_SCORE = 0.9;
  const MAX_ITERATIONS = 2;

  // Analysis Data Format
  // {
  //   leaderCollectionA: {
  //     followerCollectionX: {
  //       s12: {nrFollowers: 10, nodes: [leader, follower1, follower2, ... ]}
  //       s13: {nrFollowers: 10, nodes: [leader, follower1, follower2, ... ]}
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

  let buildShardDistribution = function (collection, distributeShardsLike) {
    let cObj = {};
    _.each(collection.shards, function (distribution, shardId) {
      cObj[shardId] = {};
      cObj[shardId].nrFollowers = distribution.length;
      cObj[shardId].distribution = distribution;
      if (distributeShardsLike) {
        cObj[shardId].distributeShardsLike = distributeShardsLike;
      }
    });

    return cObj;
  };

  let addLeaderCollection = function (collection) {
    if (isSystemCollection(collection.name)) {
      // skipping system collections
      return;
    }

    let sharding = buildShardDistribution(collection);
    if (Object.keys(sharding).length > 0) {
      return sharding;
    } else {
      print("Debug: Empty sharding for collection " + collection.name)
      return;
    }
  };

  // a "follower collection" has distributeShardsLike, will be maintained in buckets
  let addFollowerCollection = function (collection) {
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

    if (!shardBucketList[distributeShardsLike]) {
      shardBucketList[distributeShardsLike] = {
        followers: [],
        leaderDB: distributeShardsLike,
        replicationFactor: collectionReplicationFactorMap[distributeShardsLike],
        numberOfShards: collection.numberOfShards,
        shardCollectionTotalAmount: collectionReplicationFactorMap[distributeShardsLike] * collection.numberOfShards
      };
    }
    if (!shardBucketList[distributeShardsLike].followers.includes(collection.name)) {
      shardBucketList[distributeShardsLike].followers.push(collection.name);
    }
    /*
    let sharding = buildShardDistribution(collection, distributeShardsLike);
    if (Object.keys(sharding).length > 0) {
      return sharding;
    } else {
      print("Debug: Empty sharding for collection " + collection.name)
      return;
    }*/
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

    _.each(analysisData[collectionName], function (shard) {
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

    let shardDistributeInfo = calculateShardDistributionInformation(
      totalShards, collectionName, leaders, followers
    );

    let shardsWeHave = shardDistributeInfo.shardTotalAmount;
    if (shardsWeHave >= shardDistributeInfo.lowerBound && shardsWeHave <= shardDistributeInfo.upperBound) {
      // we are in that range of lowerBound <-> upperBound, almost perfect distribution
      score = 1;
    } else if (shardsWeHave == shardDistributeInfo.perfectAmountOfShards) {
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

      _.each(analysisData, function (collection, collectionName) {
        let info = calulateCollectionScore(analysisData, collectionName, dbServerName);
        let localScore = info[0];
        let distribution = info[1];

        score[dbServerName][collectionName] = {
          score: localScore,
          distribution: distribution
        };
      });
    });
    return score;
  };

  let generateAnalysis = function (agencyCollections) {
    resultData = {};

    _.each(agencyCollections, function (collections) {
      _.each(collections, function (collection) {

        if (isSystemCollection(collection.name)) {
          print("SKIPPED: " + collection.name);
          return;
        }

        if (collection.distributeShardsLike) {
          // found followers, add them to the bucket
          addFollowerCollection(collection);
          /*
          if (fResult !== null && fResult !== undefined) {
            if (!resultData[collection.name]) {
              resultData[collection.name] = {};
            }
            resultData[collection.name] = fResult;
          }*/
        } else {
          // found leaders
          let lResult = addLeaderCollection(collection);
          if (lResult !== null && lResult !== undefined) {
            if (!resultData[collection.name]) {
              resultData[collection.name] = {};
            }
            resultData[collection.name] = lResult;
          }
        }
      });
    });
    return resultData;
  };

  // function to calculate total amount of shards inside a bucket group
  let extendShardBucketList = function () {
    print("Bucket list:");
    _.each(shardBucketList, function (info, masterCollection) {
      info.shardBucketTotalAmount = info.followers.length * info.numberOfShards * info.replicationFactor;
    });
  };

  function onlyUnique(value, index, self) {
    return self.indexOf(value) === index;
  }

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
    _.each(score, function (collections, databaseServerName) {
      _.each(collections, function (collection, collectionName) {
        if (collection.score <= MIN_ALLOWED_SCORE) {
          if (!candidates[collectionName]) {
            // are we a bucket master?
            let bucketMaster = isBucketMaster(collectionName);

            candidates[collectionName] = {
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

          if (candidates[collectionName].weakestScore === null || collection.score < candidates[collectionName].weakestScore) {
            candidates[collectionName].weakestScore = collection.score;
            candidates[collectionName].weakestDatabaseServer = databaseServerName;
            candidates[collectionName].weakestAmountOfLeaders = collection.distribution.shardLeaderAmount;
            candidates[collectionName].weakestAmountOfFollowers = collection.distribution.shardFollowerAmount;
            candidates[collectionName].perfectAmountOfShards = collection.distribution.perfectAmountOfShards;
            candidates[collectionName].perfectAmountOfLeaders = collection.distribution.perfectAmountOfLeaders;
            candidates[collectionName].perfectAmountOfFollowers = collection.distribution.perfectAmountOfFollowers;
          }

          if (candidates[collectionName].bestScore === null || collection.score > candidates[collectionName].bestScore) {
            candidates[collectionName].bestScore = collection.score;
            candidates[collectionName].bestDatabaseServer = databaseServerName;
            candidates[collectionName].bestAmountOfLeaders = collection.distribution.shardLeaderAmount;
            candidates[collectionName].bestAmountOfFollowers = collection.distribution.shardFollowerAmount;
            candidates[collectionName].perfectAmountOfShards = collection.distribution.perfectAmountOfShards;
            candidates[collectionName].perfectAmountOfLeaders = collection.distribution.perfectAmountOfLeaders;
            candidates[collectionName].perfectAmountOfFollowers = collection.distribution.perfectAmountOfFollowers;
          }
        }
      });
    });

    return candidates;
  };

  moveSingleShardLocally = function (shardId, fromDBServer, toDBServer,
                                     collectionName, isLeader, analysisData, isBucketMaster) {
    // move shards in our local state only
    // debug:
    // print("Moving: " + shardId + " from: " + fromDBServer + " to: " + toDBServer + "(leader: " + isLeader + ")");

    let success = false;

    if (fromDBServer == toDBServer) {
      // makes no sense to do this
      return {
        success: success,
        data: null
      }
    }

    // modifiy local state
    if (isLeader) {
      // remove old leader, add new one
      if (analysisData[collectionName][shardId].distribution.indexOf(toDBServer) > 0) {
        // we are already follower, move not allowed
      } else {
        analysisData[collectionName][shardId].distribution.shift();
        analysisData[collectionName][shardId].distribution.unshift(toDBServer);
        success = true;
      }
    } else {
      // check that toDBServer is NOT a follower or a leader
      let toDBServerPos = analysisData[collectionName][shardId].distribution.indexOf(toDBServer);
      if (toDBServerPos === -1) {
        // we are not a follower or a leader of this shard
        let fromDBServerPos = analysisData[collectionName][shardId].distribution.indexOf(fromDBServer);
        if (fromDBServerPos === -1) {
          print("========= BAD STATE ======= ");
          print("========= NOT ALLOWED TO HAPPEN ======= ");
          print("========= BAD STATE ======= ");
        } else {
          analysisData[collectionName][shardId].distribution[fromDBServerPos] = toDBServer;
          success = true;
        }
      }
    }

    if (success) {
      // persist action history in local jobHistory
      jobHistory.push({
        action: "moveShard",
        shard: shardId,
        from: fromDBServer,
        to: toDBServer
      });
    }

    return {
      success: success,
      data: analysisData
    }
  };

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
  };

  // this function will move shards locally around and then return a new state
  let moveShardsLocally = function (candidates, analysisData) {
    // first detect the amount of what (leader/follower) to move
    _.each(candidates, function (stats, collectionName) {
      print("Candidate collection name: " + collectionName);
      let amountOfLeadersToMove = 0;
      let amountOfFollowersToMove = 0;
      let moveBucket = false;

      // special condition:
      // if we are a masterBucket collection, we need to take a look at the global
      // shard distribution per database before we start moving.
      if (stats.isBucketMaster) {
        print("WE FOUND A BUCKET MASTER !! - Name: " + collectionName);
        let amountOfTotalShardsOfBestServer = getTotalAmountOfShards(
            stats.bestDatabaseServer, analysisData, true
        );
        let amountOfTotalShardsOfWeakestServer = getTotalAmountOfShards(
            stats.weakestDatabaseServer, analysisData, true
        );

        print("Total amount of best: " + amountOfTotalShardsOfBestServer);
        print("Total amount of worst: " + amountOfTotalShardsOfWeakestServer);

        if (amountOfTotalShardsOfBestServer > amountOfTotalShardsOfWeakestServer) {
           let shardDifference = amountOfTotalShardsOfBestServer - amountOfTotalShardsOfWeakestServer;
           if (shardDifference > shardBucketList[collectionName].shardBucketTotalAmount) {
             // TODO: Check if we could calculate more precise
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

      _.each(analysisData[collectionName], function (shard, shardId) {
        if (shard.distribution[0] === stats.bestDatabaseServer) {
          // we found the best db server as leader for the current shard
          if (amountOfLeadersToMove > 0 || moveBucket) { // TODO: CHECK
            let result = moveSingleShardLocally(
                shardId, stats.bestDatabaseServer, stats.weakestDatabaseServer,
                collectionName, true, analysisData, stats.isBucketMaster
            );
            if (result.success) {
              analysisData = result.data;
              amountOfLeadersToMove--;
            }
          }
        } else {
          // we might have a follower shard
          let followerDistribution = _.clone(shard.distribution);
          followerDistribution.shift(); // remove the leader
          if (shard.distribution.indexOf(stats.bestDatabaseServer) > 0) {
            // we found dbserver as follower
            if (amountOfFollowersToMove > 0 || moveBucket) { // TODO: CHECK
              let result = moveSingleShardLocally(
                  shardId, stats.bestDatabaseServer, stats.weakestDatabaseServer,
                  collectionName, false, analysisData, stats.isBucketMaster
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

  print("=== Scores ===");
  // print(scores);

  /*
   *  Section Find Collection Candidates:
   *    Analyse the latest score and find possible move candidates
   *
   *  Builds:
   *    Populate collectionCandidates<collectionNames> array.
   */
  let candidates = getCandidatesToOptimize(scores[0]);
  print("=== Potential candidates ===");
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
  let optimizedIterations = [];
  optimizedIterations.push(moveShardsLocally(candidates, analysisData));
  scores.push(calculateCollectionsScore(analysisData));

  // the looping begins: top functions could join here as well, just wanted to keep
  // sections to better debug and comment things. can be changed later.

  for (var i = 0; i < MAX_ITERATIONS; i++) {
    candidates = getCandidatesToOptimize(scores[scores.length - 1]);
    optimizedIterations.push(moveShardsLocally(candidates, optimizedIterations[i]));
    scores.push(calculateCollectionsScore(optimizedIterations[i]));
  }

  print("===== Final Score ===== ");
  // print(scores[scores.length - 1]);

  print("Actions done: " + jobHistory.length);
  print("Iterations Done: " + MAX_ITERATIONS + " (+1)");
  /*
   *  Section Cleanup History
   *    Remove duplicates, unnecessary steps etc.
   *
   *  Builds:
   *    Write moveShards plan to file.
   */

  /*
   *  Section Create Plan:
   *    Actually create the plan, if we have found a good result distribution.
   *
   *  Builds:
   *    Write moveShards plan to file.
   */
  // we need to apply jobHistory to file
  // TODO: Still needed. We can take the functionality from the other task js file.

  print("=== Debug ===");
  print("Available DBServers: " + info.amountOfDatabaseServers);
  // print(jobHistory);
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

  //  print(scores);
  // print(shardBucketList);
  // print(analysisData);
  // print(scores);
};
