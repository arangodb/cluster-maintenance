/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango */

exports.name = 'repair-permanently-out-of-sync';
exports.group = 'cleanup tasks';
exports.args = [
  {
    name: 'run-detect',
    optional: true,
    type: 'bool',
    description: 'flag, if detection is run before'
  },
  {
    name: 'sleep',
    optional: true,
    type: 'int',
    description: 'sleep between detection and repair'
  },
  {
    name: 'repair-sharding-strategy-file',
    optional: true,
    type: 'jsonfile',
    description: 'json file created by analyze task'
  }
];
exports.args_arangosh = ' --server.endpoint COORDINATOR';
exports.description = 'Resolves permantly out of sync issue';
exports.selfTests = ['arango', 'leaderAgencyConnection'];
exports.requires = '3.3.23 - 3.11.99';
exports.info = `
Solve dis-synced shards
`;

function chooseHealthyCoordinator (planCoordinators, healthRecords) {
  const _ = require('lodash');
  let endpoint;
  _.each(planCoordinators, function (val, coordinator) {
    if (healthRecords[coordinator].Status === 'GOOD' &&
        healthRecords[coordinator].SyncStatus === 'SERVING') {
      endpoint = healthRecords[coordinator].Endpoint;
    }
  });
  return endpoint;
}

exports.run = function (extra, args) {
  const helper = require('../helper.js');

  // imports
  const _ = require('lodash');
  let shardsToFix = helper.getValue('repair-sharding-strategy-file', args) || {};
  const runDetection = helper.getValue('run-detect', args) || false;
  if (runDetection) {
    const subArgs = helper.checkArgs(extra.tasks['detect-permanently-out-of-sync'], ['detect-permanently-out-of-sync', 'true', 'false']);
    extra.tasks['detect-permanently-out-of-sync'].run(extra, subArgs);
    try {
      shardsToFix = JSON.parse(require('fs').readFileSync('shard-sync-mismatch.json'));
    } catch (err) {
      print('Could not read mismatch file, probably nothing to fix.');
    }
    const sleepTime = helper.getValue('sleep', args) || 0;
    if (!_.isEmpty(shardsToFix) && sleepTime > 0) {
      print('Waiting', sleepTime, 'seconds before proceeding with repair...');
      require('internal').wait(sleepTime);
    }
  }

  if (_.isEmpty(shardsToFix)) {
    print('Nothing to fix!');
    return;
  }

  // send supervision to maintenance mode
  // get an agency dump
  const conf = helper.getAgencyDumpFromObjectOrAgency()[0];
  const agLeader = arango.getEndpoint();
  const planCollections = conf.arango.Plan.Collections;

  const coordinatorEndpoint =
        chooseHealthyCoordinator(conf.arango.Plan.Coordinators, conf.arango.Supervision.Health);

  arango.reconnect(coordinatorEndpoint, '_system');
  arango.POST('/_admin/backup/create', {});

  _.each(shardsToFix, function (collections, database) {
    if (Object.prototype.hasOwnProperty.call(planCollections, database)) { // or else database gone
      _.each(collections, function (shards, cid) {
        if (Object.prototype.hasOwnProperty.call(planCollections[database], cid)) { // or else database gone
          _.each(shards, function (servers, shard) {
            const planServers = _.cloneDeep(planCollections[database][cid].shards[shard]);
            const original = _.cloneDeep(planServers);
            const planLeader = planServers[0];
            const checkLeader = servers[0].server;
            const checkValue = servers[0].check;
            if (planLeader === checkLeader) { // Leader must still be the same as for the checks
              _.each(servers, function (serverData) {
                const server = serverData.server;
                const check = serverData.check;
                if (server !== planLeader && check !== checkValue) {
                  print('repairing shard ' + shard + '...');

                  arango.reconnect(coordinatorEndpoint, '_system');
                  print('deactivating supervision ...');
                  arango.PUT('/_admin/cluster/maintenance', '"on"');
                  print('... done');
                  arango.reconnect(agLeader, '_system');

                  const planPath = '/arango/Plan/Collections/' + database + '/' + cid + '/shards/' + shard;
                  const curPath = '/arango/Current/Collections/' + database + '/' + cid + '/' + shard + '/servers';
                  planServers.splice(planServers.indexOf(server), 1);
                  const removeFollowerTrx = [
                    { [planPath]: planServers, 'arango/Plan/Version': { op: 'increment' } }, { [planPath]: original }];
                  let result = arango.POST('/_api/agency/write', [removeFollowerTrx]);
                  if (typeof (result) === 'object' && Object.prototype.hasOwnProperty.call(result, 'results') &&
                      typeof (result.results) === 'object' && result.results[0] > 0) { // result[] must exist
                    print('follower ' + server + ' stripped from plan');
                    let start = require('internal').time();
                    let planServersSorted = _.cloneDeep(planServers);
                    planServersSorted.sort();
                    print('waiting for dropped follower ');
                    while (require('internal').time() - start < 120.0) {
                      const currentServers = arango.POST('/_api/agency/read', [[curPath]])[0]
                        .arango.Current.Collections[database][cid][shard].servers;
                      currentServers.sort();
                      if (_.isEqual(planServersSorted, currentServers)) {
                        break;
                      }
                      require('internal').sleep(0.1);
                    }

                    planServers.push(server);
                    planServersSorted = _.cloneDeep(planServers);
                    planServersSorted.sort();
                    print('readding follower ' + server);
                    const readdFollowerTrx = [
                      { [planPath]: original, 'arango/Plan/Version': { op: 'increment' } }];
                    result = arango.POST('/_api/agency/write', [readdFollowerTrx]);
                    if (typeof (result) === 'object' && Object.prototype.hasOwnProperty.call(result, 'results') &&
                        typeof (result.results) === 'object' && result.results[0] > 0) { // result[] must exist
                      arango.reconnect(coordinatorEndpoint, '_system');
                      print('re-activating supervision ...');
                      arango.PUT('/_admin/cluster/maintenance', '"off"');
                      print('... done');
                      arango.reconnect(agLeader, '_system');

                      start = require('internal').time();
                      print('waiting for synchronous replication to get follower in sync again ');
                      while (require('internal').time() - start < 36000.0) {
                        const currentServers = arango.POST('/_api/agency/read', [[curPath]])[0]
                          .arango.Current.Collections[database][cid][shard].servers;
                        currentServers.sort();
                        if (_.isEqual(planServersSorted, currentServers)) {
                          break;
                        }
                        require('internal').sleep(1);
                      }
                      print(' ... done');
                      print();
                    } else {
                      print('\n\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n\n');
                      helper.printBad('Failed to send the following transaction to agency: ');
                      print('   ', JSON.stringify([readdFollowerTrx]));
                      print('\n... bailing out. ATTENTION: Cluster remains in maintenance mode. The failed transaction MUST BE APPLIED UNDER ALL CIRMCUMSTANCES!!!');
                      print('You can use the following curl command:\n');
                      print('curl -H"$(arangodb auth header --auth.jwt-secret=/secrets/cluster/jwt/token)" ' + agLeader + "/_api/agency/write -d '" + JSON.stringify([readdFollowerTrx]) + "'\n");
                      print('(If the agency leader has changed in the meantime, you have to send it to the new one!)');
                      print('\nFurthermore, we have left the cluster in maintenance mode for another hour!');

                      print('\n\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n\n');
                      process.exit(1);
                    }
                  } else {
                    helper.printBad('Failed to send the following transaction to agency: ');
                    print(removeFollowerTrx);
                    print('... ignoring shard', shard, 'continuing...');
                  }
                }
              });
            } else {
              helper.printBad('Planned leader for shard ' + shard +
                              ' is not longer the planned leader durig the detection. Skipping this shard');
            }
          });
        }
      });
    } else {
      helper.printBad('Database ' + database + ' no longer planned');
    }
  });

  arango.reconnect(coordinatorEndpoint, '_system');
  print('reactivating supervision ...');
  arango.PUT('/_admin/cluster/maintenance', '"off"');
  print('... done');
};
