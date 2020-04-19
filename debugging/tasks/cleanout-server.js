/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango, db */
exports.name = "cleanout-server";
exports.group = "move shard tasks";
exports.args = [
  {
    "name": "server",
    "optional": false,
    "type": "string",
    "description": "database server"
  }
];
exports.args_arangosh = "| --server.endpoint AGENT";
exports.description = "Creates analysis for a plan to rebalance shards in your cluster.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.6.0 - 3.7.99";
exports.info = `
This task cleans out a server and remove it from the list of DBservers.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');
  const internal = require('internal');

  // at what level shall we disply the information
  const serverId = helper.getValue("server", args);

  if (serverId.substring(0, 4) !== 'PRMR') {
    helper.fatal("expecting a database server, got '" + serverId + "'");
  }

  // imports
  const _ = require('lodash');

  // get an agency dump
  const conf = helper.getAgencyDumpFromObjectOrAgency()[0];
  const health = conf.arango.Supervision.Health;

  if (!health.hasOwnProperty(serverId)) {
    helper.fatal("unknown database server, got '" + serverId + "'");
  }

  const status = health[serverId].Status;

  if (status !== 'GOOD') {
    helper.fatal("database server '" + serverId + "' is " + status);
  }

  const shortName = health[serverId].ShortName;

  // show a server list
  helper.showServers(conf, helper.getAgencyConfiguration());

  // find a healthy coordinator
  const cord = _.find(health, function (o, id) {
    return o.Status === 'GOOD' && id.substring(0, 4) === 'CRDN';
  });

  if (!cord) {
    helper.fatal("cannot find a health coordinator");
  }

  arango.reconnect(cord.Endpoint, "_system");

  // cleanout server
  const data = {server: shortName};
  let res = helper.httpWrapper('POST', '/_admin/cluster/cleanOutServer', data);

  if (res.code !== 202) {
    helper.fatal("cleanout failed: ", JSON.stringify(res));
  }

  const jobId = res.id;
  print(res);

  const dblist = db._databases();
  const sleep = 10;

  print("INFO checking shard distribution every " + sleep + " seconds...");

  res = helper.httpWrapper('GET', '/_api/cluster/agency-dump');
  print(res);
    
  let count;
  do {
    count = 0;

    for (dbase in dblist) {
        const sd = arango.GET("/_db/" + dblist[dbase] + "/_admin/cluster/shardDistribution");
        const collections = sd.results;

        for (collection in collections) {
          const current = collections[collection].Current;

          for (shard in current) {
            if (current[shard].leader == shortName) {
              ++count;
            }
          }
        }
    }

    print("INFO shards to be moved away from node " + shortName + ": " + count);
    if (count == 0) break;
    internal.wait(sleep);
  } while (count > 0);
};
