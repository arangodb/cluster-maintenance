/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */

exports.name = "repair-permanently-out-of-sync";
exports.group = "cleanup tasks";
exports.args = [
  {
    name: "run-detect",
    optional: true,
    type: "bool",
    description: "flag, if detection is run before"
  },
  {
    name: "repair-sharding-strategy-file",
    optional: true,
    type: "jsonfile",
    description: "json file created by analyze task"
  }
];
exports.args_arangosh = " --server.endpoint COORDINATOR";
exports.description = "Resolves permantly out of sync issue";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.8.99";
exports.info = `
Solve dis-synced shards
`;

function chooseHealthyCoordinator(planCoordinators, healthRecords) {
  const _ = require('lodash');
  var endpoint;
  _.each (planCoordinators, function(val, coordinator)  {
    if (healthRecords[coordinator].Status === "GOOD" &&
        healthRecords[coordinator].SyncStatus === "SERVING") {
      endpoint = healthRecords[coordinator].Endpoint;
      return;
    }
  });
  return endpoint;
}

exports.run = function (extra, args) {
  const helper = require('../helper.js');

  // imports
  const _ = require('lodash');
  let shardsToFix = helper.getValue("repair-sharding-strategy-file", args) || {};
  let runDetection = helper.getValue("run-detect", args) || false;
  if (runDetection) {
    let subArgs = helper.checkArgs(extra.tasks["detect-permanently-out-of-sync"], ["detect-permanently-out-of-sync", "true", "false"]);
    print("Hugo:", subArgs);
    extra.tasks["detect-permanently-out-of-sync"].run(extra, subArgs);
    try {
      shardsToFix = JSON.parse(require("fs").readFileSync("shard-sync-mismatch.json"));
    } catch (err) {
      print("Could not read mismatch file, probably nothing to fix.");
    }
  }

  if (_.isEmpty(shardsToFix)) {
    print("Nothing to fix!");
    return;
  }

  // send supervision to maintenance mode
  // get an agency dump
  var conf = helper.getAgencyDumpFromObjectOrAgency()[0];
  const agLeader = arango.getEndpoint();
  const planCollections = conf.arango.Plan.Collections;

  const coordinatorEndpoint =
        chooseHealthyCoordinator(conf.arango.Plan.Coordinators, conf.arango.Supervision.Health);

  arango.reconnect(coordinatorEndpoint, "_system");
  let backup = arango.POST("/_admin/backup/create", {});

  print ("deactivating supervision ...")
  let maintenance = arango.PUT('/_admin/cluster/maintenance', '"on"');
  print ("... done")
  const coordinator = arango.getEndpoint();

  arango.reconnect(agLeader, "_system");

  _.each(shardsToFix, function(collections, database) {
    if (planCollections.hasOwnProperty(database)) { // or else database gone
      _.each(collections, function(shards, cid) {
        if (planCollections[database].hasOwnProperty(cid)) { // or else database gone
          _.each(shards, function(servers, shard) {
            var planServers = _.cloneDeep(planCollections[database][cid].shards[shard]);
            let original = _.cloneDeep(planServers);
            let planLeader = planServers[0];
            let checkLeader = servers[0].server;
            let checkValue = servers[0].check;
            if (planLeader == checkLeader)  {                      // Leader must still be the same as for the checks
              _.each(servers, function(serverData) {
                let server = serverData.server;
                let check = serverData.check;
                if (server != planLeader && check != checkValue) { // TODO FIX
                  print ("repairing shard " + shard + "...");
                  let planPath = "/arango/Plan/Collections/" + database + "/" + cid + "/shards/" + shard;
                  let curPath = "/arango/Current/Collections/" + database +"/" + cid + "/" + shard + "/servers";
                  planServers.splice(planServers.indexOf(server), 1);
                  let removeFollowerTrx = [
                    {[planPath] : planServers, "arango/Plan/Version" : {"op" : "increment"}}, {[planPath] : original}];
                  var result;
                  var result = arango.POST("/_api/agency/write", [removeFollowerTrx]); // TODO sanity check
                  if (typeof(result) == "object" && result.hasOwnProperty("results") &&
                      typeof(result.results) == "object" &&  result.results[0] > 0) { // result[] must exist
                    print ("follower " + server +  " stripped from plan");
                    let start = require('internal').time();
                    let planServersSorted = _.cloneDeep(planServers);
                    planServersSorted.sort();
                    print ("waiting for current ");
                    while(require('internal').time() - start < 120.0) {
                      let currentServers = arango.POST("/_api/agency/read", [[curPath]])[0].
                          arango.Current.Collections[database][cid][shard].servers;
                      currentServers.sort();
                      if (_.isEqual(planServersSorted, currentServers)) {
                        break;
                      }
                      require('internal').sleep(0.1);
                    }
                    planServers.push(server);
                    planServersSorted = _.cloneDeep(planServers);
                    planServersSorted.sort();
                    print ("readding follower " + server)
                    let readdFollowerTrx =  [
                      {[planPath] : planServers, "arango/Plan/Version" : {"op" : "increment"}}];
                    result = arango.POST("/_api/agency/write", [readdFollowerTrx]); // TODO sanity check
                    if (typeof(result) == "object" && result.hasOwnProperty("results") &&
                        typeof(result.results) == "object" &&  result.results[0] > 0) { // result[] must exist
                      start = require('internal').time();
                      print ("waiting for current ");
                      while(require('internal').time() - start < 3600.0) {
                        let currentServers = arango.POST("/_api/agency/read", [[curPath]])[0].
                            arango.Current.Collections[database][cid][shard].servers;
                        currentServers.sort();
                        if (_.isEqual(planServersSorted, currentServers)) {
                          break;
                        }
                        require('internal').sleep(1);
                      }
                      print (" ... done");
                      print ();
                    } else {
                      helper.printBad("Failed to send the following transaction to agency: ");
                      print(readdFollowerTrx);
                      print("... bailing out. ATTENTION: Cluster remains in maintenance mode. The failed transaction MUST UNDER ALL CIRMCUMTANCES");
                      process.exit(1);
                    }
                  } else {
                    helper.printBad("Failed to send the following transaction to agency: ");
                    print(removeFollowerTrx);
                    print("... bailing out");
                  }
                }
              });
            } else {
              helper.printBad("Planned leader for shard " + shard +
                              " is not longer the planned leader durig the detection. Skipping this shard");
            }
          });
        }
      });
    } else {
      helper.printBad('Database ' + database + ' no longer planned');
    }
  });

  arango.reconnect(coordinatorEndpoint, "_system");
  print ("reactivating supervision ...")
  maintenance = arango.PUT('/_admin/cluster/maintenance', '"off"');
  print ("... done")

};
