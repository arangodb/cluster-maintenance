/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */
exports.run = function (extra, args, cleanout) {
  const helper = require('./helper.js');
  const internal = require('internal');
  const printBad = helper.printBad;

  // imports
  const _ = require('lodash');

  // get an agency dump
  const conf = helper.getAgencyDumpFromObjectOrAgency()[0];

  // at what level shall we disply the information
  const name = helper.getValue("server", args);
  const { serverId, shortName } = helper.findServer(conf, name);

  if (serverId.substring(0, 4) !== 'PRMR') {
    helper.fatal("expecting a database server, got '" + name + "'");
  }

  // check the health of the server
  const health = conf.arango.Supervision.Health;

  if (!health.hasOwnProperty(serverId)) {
    helper.fatal("unknown database server, got '" + serverId + "'");
  }

  const status = health[serverId].Status;

  if (status !== 'GOOD') {
    helper.fatal("database server '" + serverId + "' is " + status);
  }

  // show a server list
  helper.showServers(conf, helper.getAgencyConfiguration());

  // find a healthy coordinator
  const cord = _.find(health, function (o, id) {
    return o.Status === 'GOOD' && id.substring(0, 4) === 'CRDN';
  });

  if (!cord) {
    helper.fatal("cannot find a healthy coordinator");
  }

  arango.reconnect(cord.Endpoint, "_system");

  // cleanout server
  const data = {server: shortName};
  let res;
  let task = "unknown";

  if (cleanout) {
    print("Starting cleanOutServer");
    res = helper.httpWrapper('POST', '/_admin/cluster/cleanOutServer', data);
    task = "cleanout";
  } else {
    print("Starting resignLeadership");
    res = helper.httpWrapper('POST', '/_admin/cluster/resignLeadership', data);
    task = "resign";
  }

  if (res.code !== 202) {
    helper.fatal(task + " failed: ", JSON.stringify(res));
  }

  const jobId = res.id;

  const dblist = db._databases();
  const sleep = 60;

  print("INFO checking shard distribution every " + sleep + " seconds...");

  let count;
  let leaderOnly;
  let t1 = new Date();

  do {
    count = 0;
    leaderOnly = 0;

    res = helper.httpWrapper('GET', '/_admin/cluster/queryAgencyJob?id=' + jobId);

    if (res.status === 'Failed') {
      printBad(res.job.reason);
      helper.fatal("cannot " + task + " server: " + res.job.reason);
    }

    for (let dbase in dblist) {
      const sd = arango.GET("/_db/" + dblist[dbase] + "/_admin/cluster/shardDistribution");
      const collections = sd.results;

      for (let collection in collections) {
        const current = collections[collection].Current;

        for (let shard in current) {
          const s = current[shard];

          if (s.leader === shortName) {
            ++count;
          }

          if (cleanout) {
            _.each(s.followers, function (key) {
              if (key === shortName) {
                ++count;
              }
            });
          } else {
            if (s.followers.length === 0 && s.leader === shortName) {
              ++leaderOnly;
            }
          }
        }
      }
    }

    const t2 = new Date();
    print("INFO " + (t2));

    if (cleanout) {
      print("INFO " + task + ": shards to be moved away from node " + shortName + ": " + count);
      if (count === 0) break;
    } else {
      print("INFO " + task + ": shards to be moved away from node " + shortName + ": " + count +
            ", leader-only: " + leaderOnly);
      if (count === leaderOnly) break;
    }

    const d = (t2 - t1) / 1000;
    t1 = t2;

    if (d < sleep) {
      internal.wait(sleep - d);
    }
  } while (count > 0);

  while (res.status === 'ToDo') {
    res = helper.httpWrapper('GET', '/_admin/cluster/queryAgencyJob?id=' + jobId);

    const t2 = new Date();
    print("INFO " + (t2));
    print("INFO waiting for job to finished");

    internal.wait(sleep / 10);
  }
};
