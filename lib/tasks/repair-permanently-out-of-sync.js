/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */

exports.name = "repair-permanently-out-of-sync";
exports.group = "cleanup tasks";
exports.args = [
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
  let shardsToFix = helper.getValue("repair-sharding-strategy-file", args) ||
      "shard-sync-mismatch.json";

  // send supervision to maintenance mode
  // get an agency dump
  var conf = helper.getAgencyDumpFromObjectOrAgency()[0];
  const agLeader = arango.getEndpoint();
  const planCollections = conf.arango.Plan.Collections;

  const coordinatorEndpoint =
        chooseHealthyCoordinator(conf.arango.Plan.Coordinators, conf.arango.Supervision.Health);

  arango.reconnect(coordinatorEndpoint, "_system");
  let backup = arango.POST("/_admin/backup/create", {});
  
  let maintenance = arango.PUT('/_admin/cluster/maintenance', '"on"');
  const coordinator = arango.getEndpoint();

  arango.reconnect(agLeader, "_system");

  _.each(shardsToFix, function(collections, database) {
    if (planCollections.hasOwnProperty(database)) { // or else database gone
      _.each(collections, function(shards, cid) {
        if (planCollections[database].hasOwnProperty(cid)) { // or else database gone
          _.each(shards, function(servers, shard) {
            var planServers = _.cloneDeep(planCollections[database][cid].shards[shard]);
            let original = _.cloneDeep(planServers);
            let shardLeader = planServers[0];
            let leaderCheck = servers[shardLeader];
            _.each(servers, function(check, server) {
              if (server != shardLeader && check == leaderCheck) { // TODO FIX
                let planPath = "/arango/Plan/Collections/" + database + "/" + cid + "/shards/" + shard;
                let curPath = "/arango/Current/Collections/" + database +"/" + cid + "/" + shard + "/servers";
                planServers.splice(planServers.indexOf(server), 1);
                let removeFollowerTrx = [
                  {[planPath] : planServers, "arango/Plan/Version" : {"op" : "increment"}}, {[planPath] : original}];
                print ([removeFollowerTrx]);
                var result;
                var result = arango.POST("/_api/agency/write", [removeFollowerTrx]); // TODO sanity check
                if (typeof(result) == "object" && result.hasOwnProperty("results") &&
                    typeof(result.results) == "object" &&  result.results[0] > 0) { // result[] must exist
                  let start = require('internal').time();
                  let planServersSorted = _.cloneDeep(planServers);
                  planServersSorted.sort();
                  while(require('internal').time() - start < 1.0) {
                    let currentServers = arango.POST("/_api/agency/read", [[curPath]])[0].
                        arango.Current.Collections[database][cid][shard].servers;
                    currentServers.sort();
                    print(planServersSorted +" "+ currentServers);
                    if (_.isEqual(planServersSorted, currentServers)) {
                      break;
                    }
                    require('internal').sleep(0.1);
                  }
                  planServers.push(server);
                  planServersSorted = _.cloneDeep(planServers);
                  planServersSorted.sort();
                  let readdFollowerTrx =  [
                    {[planPath] : planServers, "arango/Plan/Version" : {"op" : "increment"}}];
                  print([readdFollowerTrx]);
                  result = arango.POST("/_api/agency/write", [readdFollowerTrx]); // TODO sanity check
                  if (typeof(result) == "object" && result.hasOwnProperty("results") &&
                    typeof(result.results) == "object" &&  result.results[0] > 0) { // result[] must exist
                    start = require('internal').time();
                    while(require('internal').time() - start < 1.0) {
                      let currentServers = arango.POST("/_api/agency/read", [[curPath]])[0].
                          arango.Current.Collections[database][cid][shard].servers;
                      currentServers.sort();
                      print(_.isEqual(planServersSorted, currentServers));
                      if (_.isEqual(planServersSorted, currentServers)) {
                        break;
                      }
                      require('internal').sleep(0.1);
                    }
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
          });
        }
      });
    } else {
      helper.printBad('Database ' + database + ' no longer planned');
    }
  });
  
  arango.reconnect(coordinatorEndpoint, "_system");
  maintenance = arango.PUT('/_admin/cluster/maintenance', '"off"');
  
};
