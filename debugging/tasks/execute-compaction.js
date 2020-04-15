/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */
exports.name = "execute-compaction";
exports.group = "analyze tasks";
exports.args = [
  {
    "name": "server",
    "optional": false,
    "type": "string",
    "description": "server to compact"
  }
];
exports.args_arangosh = " --server.endpoint AGENT";
exports.description = "Run compaction on server";
exports.selfTests = ["arango", "db", "agencyConnection"];
exports.requires = "3.3.23 - 3.7.99";
exports.info = `
Runs the compaction on all collections on one server.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');

  // which database to compact
  let server = helper.getValue("server", args);

  // imports
  const _ = require('lodash');

  // get an agency dump
  const conf = helper.getAgencyDumpFromObjectOrAgency()[0];
  const shardMap = conf.arango.Plan.Collections;
  const health = conf.arango.Supervision.Health;
  const info = {};
  const serverMap = {};

  _.each(conf.arango.Current.Collections, function (val, dbname) {
    info[dbname] = {};

    _.each(val, function (val, cid) {
      const cname = shardMap[dbname][cid].name;
      info[dbname][cname] = {};

      _.each(val, function (val, shard) {
        const servers = val.servers;
        info[dbname][cname][shard] = [];

        _.each(servers, function (server) {
          const s = health[server];
          const status = s.Status;
          const ip = s.Endpoint;
          const sinfo = {server, status, ip, dbname, cid, cname, shard};
          info[dbname][cname][shard].push(sinfo);

          if (!serverMap.hasOwnProperty(server)) {
            serverMap[server] = [];
          }

          serverMap[server].push(sinfo);
        });
      });
    });
  });

  _.each(serverMap, function (val, id) {
    if (server === '*' || id === server) {
      const ip = val[0].ip;
      arango.reconnect(ip, "_system");

      print("- using server '" + id + "' at '" + ip + "'");

      _.each(val, function (entry) {
        db._useDatabase(entry.dbname);
        const collection = db._collection(entry.shard);

        print("-- " + entry.dbname + "/" + entry.cname + "/" + entry.shard);
        try {
          collection.compact();
        } catch (ex) {
        }
      });
    }
  });
};
