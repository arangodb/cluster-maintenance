/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango  */

exports.name = 'detect-permanently-out-of-sync';
exports.group = 'analyze tasks';
exports.args = [
  {
    name: 'exclusive-locking',
    optional: true,
    type: 'bool',
    description: 'should exclusive locking be used for later runs'
  },
  {
    name: 'calculate-hashes',
    optional: true,
    type: 'bool',
    description: 'calculate hashes for comparison rather than counts only'
  }
];
exports.args_arangosh = ' --server.endpoint COORDINATOR';
exports.description = 'Shows multiple levels of intrusive synchronization issue detection';
exports.selfTests = ['arango', 'leaderAgencyConnection'];
exports.requires = '3.3.23 - 3.10.99';
exports.info = `
Multiple levels of intrusive synchronization issue detection.
`;

function openStreamingTrx (coordinatorEndpoint, database, collection) {
  // Run one transaction touching multiple documents:
  arango.reconnect(coordinatorEndpoint, database);
  const trx = arango.POST('/_api/transaction/begin',
    { collections: { exclusive: [collection] } });
  for (let i = 0; i < 100; ++i) {
    arango.POST(`/_api/document/${collection}`, { Hallo: i }, { 'x-arango-trx-id': trx.result.id });
  }
  return trx.result.id;
}

function closeStreamingTrx (coordinatorEndpoint, database, trxId) {
  arango.reconnect(coordinatorEndpoint, database);
  arango.DELETE(`/_api/transaction/${trxId}`);
}

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

function detectCountMismatch (agLeader, conf) {
  const n = 5;
  const _ = require('lodash');
  const AsciiTable = require('../3rdParty/ascii-table');
  const helper = require('../helper.js');
  const internal = require('internal');
  const shardToCol = {};
  let mismatch = {};

  for (let i = 0; i < n; ++i) {
    if (i === 0) {
      arango.reconnect(agLeader, '_system');
      conf = helper.getAgencyDumpFromObjectOrAgency()[0];
    }

    const planDBServers = conf.arango.Plan.DBServers;
    const planDatabases = conf.arango.Plan.Databases;
    const planCollections = conf.arango.Plan.Collections;
    const currentCollections = conf.arango.Current.Collections;
    const planShards = {};
    const curShards = {};

    _.each(planCollections, function (database, dbname) {
      _.each(database, function (collection, cname) {
        _.each(collection.shards, function (shard, shname) {
          if (!Object.prototype.hasOwnProperty.call(planShards, dbname)) {
            planShards[dbname] = {};
          }
          shardToCol[shname] = { name: collection.name, cid: cname };
          planShards[dbname][shname] = shard;
        });
      });
    });

    _.each(currentCollections, function (database, dbname) {
      _.each(database, function (collection, cname) {
        _.each(collection, function (shard, shname) {
          if (!Object.prototype.hasOwnProperty.call(curShards, dbname)) {
            curShards[dbname] = {};
          }
          curShards[dbname][shname] = shard.servers;
        });
      });
    });

    const counts = {};
    const labels = {};
    const shorts = {};
    const shards = (i === 0) ? planShards : mismatch;

    _.each(planDBServers, function (val, dbserver) {
      const ip = conf.arango.Supervision.Health[dbserver].Endpoint;
      arango.reconnect(ip, '_system');
      _.each(planDatabases, function (val, database) {
        const localDB = (i === 0)
          ? arango.GET('/_db/' + database + '/_api/collection').result
          : mismatch[database];

        _.each(localDB, function (shard) {
          if (!shard.name.startsWith('_statistics') && Object.prototype.hasOwnProperty.call(shards[database], shard.name)) {
            const c = arango.GET('/_db/' + database + '/_api/collection/' + shard.name + '/count').count;
            if (!Object.prototype.hasOwnProperty.call(counts, database)) {
              counts[database] = {};
              labels[database] = {};
              shorts[database] = {};
            }
            if (planShards[database][shard.name].indexOf(dbserver) >= 0 &&
                curShards[database][shard.name].indexOf(dbserver) >= 0) { // still planned and in sync
              if (!Object.prototype.hasOwnProperty.call(counts[database], shard.name)) {
                counts[database][shard.name] = [c];
                labels[database][shard.name] = [dbserver];
                shorts[database][shard.name] = [conf.arango.Supervision.Health[dbserver].ShortName];
              } else {
                counts[database][shard.name].push(c); // keep track of counts
                labels[database][shard.name].push(dbserver);
                shorts[database][shard.name].push(conf.arango.Supervision.Health[dbserver].ShortName);
              }
            }
          }
        });
      });
    });

    if (i !== 0) {
      mismatch = {};
    }
    _.each(counts, function (val, database) {
      _.each(val, function (replica, shard) {
        const nr = replica.length;
        if (nr > 1) {
          const c = replica[0];
          for (let j = 1; j < nr; ++j) {
            if (replica[j] !== c) {
              if (!Object.prototype.hasOwnProperty.call(mismatch, database)) {
                mismatch[database] = {};
              }
              if (!Object.prototype.hasOwnProperty.call(mismatch[database], shard)) {
                mismatch[database][shard] = { name: shard, counts: replica, servers: labels[database][shard], shorts: shorts[database][shard] };
                break;
              }
            }
          }
        }
      });
    });

    if (_.isEmpty(mismatch)) {
      break;
    }

    internal.wait(i * 0.1);
  }

  const toWork = {};
  if (!_.isEmpty(mismatch)) {
    helper.printBad('Your cluster has non-syncing shards');
    const table = new AsciiTable('Non-syncing shards');
    table.setHeading('Database', 'Collection', 'Shard', 'Database servers', 'Counts');
    _.each(mismatch, function (shards, database) {
      _.each(shards, function (shard) {
        if (!Object.prototype.hasOwnProperty.call(toWork, database)) {
          toWork[database] = {};
        }
        if (!Object.prototype.hasOwnProperty.call(toWork[database], shardToCol[shard.name].name)) {
          toWork[database][shardToCol[shard.name].cid] = [shard.name];
        } else {
          toWork[database][shardToCol[shard.name].cid].push(shard.name);
        }
        table.addRow(
          database, shardToCol[shard.name].name, shardToCol[shard.name].cid + '/' + shard.name,
          mismatch[database][shard.name].shorts, mismatch[database][shard.name].counts);
      });
    });
    print(table.toString());
  } else {
    helper.printGood('Your cluster does not have any non-syncing shards');
  }
  return toWork;
}

function detectExclusiveMismatch (coordinatorEndpoint, agLeader, conf, toWork, sturdy) {
  const _ = require('lodash');
  const AsciiTable = require('../3rdParty/ascii-table');
  const helper = require('../helper.js');
  const planCollections = conf.arango.Plan.Collections;
  const healthRecords = conf.arango.Supervision.Health;
  const fs = require('fs');
  const mismatch = {};

  _.each(toWork, function (collections, database) {
    _.each(collections, function (shards, collection) {
      const trxId = openStreamingTrx(coordinatorEndpoint, database, collection);
      _.each(shards, function (shard) {
        const checks = {};
        const jobs = [];
        _.each(planCollections[database][collection].shards[shard], function (server) {
          arango.reconnect(healthRecords[server].Endpoint, '_system');
          jobs.push({
            shortname: healthRecords[server].ShortName,
            server: server,
            endpoint: healthRecords[server].Endpoint,
            jobId: arango.GET_RAW(
              '/_db/' + database + '/_api/collection/' + shard + ((sturdy) ? '/checksum' : '/count'),
              { 'x-arango-async': 'store' }).headers['x-arango-async-id']
          });
        });
        for (let i = 0; i < jobs.length; ++i) {
          arango.reconnect(jobs[i].endpoint, '_system');
          while (true) {
            const resp = arango.PUT(`/_api/job/${jobs[i].jobId}`, {});
            if (resp.code === 200) {
              checks[jobs[i].server] = sturdy ? resp.checksum : resp.count;
              break;
            }
            require('internal').wait(0.1);
          }
        }
        let last = '0';
        let i = 0;
        let off = false;
        _.each(checks, function (check, shortname) {
          if (i > 0) {
            if (last !== check) {
              off = true;
              return;
            }
          }
          last = check;
          ++i;
        });
        if (off) {
          if (!Object.prototype.hasOwnProperty.call(mismatch, database)) {
            mismatch[database] = {};
          }
          if (!Object.prototype.hasOwnProperty.call(mismatch[database], collection)) {
            mismatch[database][collection] = {};
          }
          mismatch[database][collection][shard] = [];
          _.each(planCollections[database][collection].shards[shard], function (server) {
            mismatch[database][collection][shard].push({ server: server, check: checks[server] });
          });
        }
      });
      closeStreamingTrx(coordinatorEndpoint, database, trxId);
    });
  });

  if (!_.isEmpty(mismatch)) {
    helper.printBad('Your cluster has non-syncing shards');
    const table = new AsciiTable('Non-syncing shards');
    table.setHeading('Database', 'Collection', 'Shard', 'Database servers');
    _.each(mismatch, function (collections, database) {
      _.each(collections, function (shards, collection) {
        _.each(shards, function (hashes, shard) {
          table.addRow(database, conf.arango.Plan.Collections[database][collection].name, shard);
        });
      });
    });
    print(table.toString());
    fs.write('shard-sync-mismatch.json', JSON.stringify(mismatch));
  }
}

exports.run = function (extra, args) {
  const helper = require('../helper.js');

  // imports
  const _ = require('lodash');
  const exclusive = helper.getValue('exclusive-locking', args) || false;
  const sturdy = helper.getValue('calculate-hashes', args) || false;

  // get an agency dump
  const conf = helper.getAgencyDumpFromObjectOrAgency()[0];
  const agLeader = arango.getEndpoint();

  // Coordinators
  const coordinatorEndpoint =
        chooseHealthyCoordinator(conf.arango.Plan.Coordinators, conf.arango.Supervision.Health);

  const toWork = detectCountMismatch(agLeader, conf);

  if (exclusive && !_.isEmpty(toWork)) {
    detectExclusiveMismatch(coordinatorEndpoint, agLeader, conf, toWork, sturdy);
  }

  arango.reconnect(coordinatorEndpoint, '_system');
};
