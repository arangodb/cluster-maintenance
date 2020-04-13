/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */
exports.name = "show-documents";
exports.group = "analyze tasks";
exports.args = [];
exports.args_arangosh = " --server.endpoint AGENT";
exports.description = "Show number of documents.";
exports.selfTests = ["arango", "db", "agencyConnection"];
exports.requires = "3.3.23 - 3.7.99";
exports.info = `
Show the number of documents in all collections in all databases.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');

  // imports
  const _ = require('lodash');
  const AsciiTable = require('../3rdParty/ascii-table');

  // get an agency dump
  const conf = helper.getAgencyDumpFromObjectOrAgency()[0];
  const shardMap = conf.arango.Plan.Collections;
  const health = conf.arango.Supervision.Health;
  const info = {};
  const serverMap = {};

  const serverCount = {};
  let scount = 0;

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
            serverCount[server] = ++scount;
          }

          serverMap[server].push(sinfo);
        });
      });
    });
  });

  scount++;

  _.each(serverMap, function (val, id) {
    const ip = val[0].ip;
    arango.reconnect(ip, "_system");

    _.each(val, function (entry) {
      db._useDatabase(entry.dbname);
      const collection = db._collection(entry.shard);
      const count = collection.count();

      entry.count = count;
    });
  });

  const keys = _.sortBy(_.keys(info));

  const table1 = new AsciiTable('Leader and Follower');
  const header = ['database', 'collection', 'shard', 'count'];
  const offset = header.length;

  _.each(serverCount, function (pos, name) {
    header[offset + pos - 1] = name;
  });

  table1.setHeading(header);

  _.each(keys, function (dbname) {
    const dbs = info[dbname];
    const keys = _.sortBy(_.keys(dbs));
    let countDatabase = (new Array(scount)).fill(0);
    let rows1 = [];

    _.each(keys, function (cname) {
      const shards = dbs[cname];
      const keys = _.sortBy(_.keys(shards));
      let countCollection = (new Array(scount)).fill(0);
      let rows2 = [];

      _.each(keys, function (sname) {
        const servers = shards[sname];
        let countServers = (new Array(scount)).fill(0);

        _.each(servers, function (server) {
          const name = server.server;
          const count = server.count;

          countServers[0] += count;
          countServers[serverCount[name]] += count;
        });

        rows2.push(_.concat([dbname, cname, sname], countServers));

        countCollection = _.zipWith(countCollection, countServers, (a, b) => a + b);
      });

      rows1.push(_.concat([dbname, cname, ''], countCollection));
      _.each(rows2, (row) => rows1.push(row));

      countDatabase = _.zipWith(countDatabase, countCollection, (a, b) => a + b);
    });

    table1.addRow(_.concat([dbname, '', ''], countDatabase));

    _.each(rows1, (row) => {
      table1.addRow(row);
    });

    table1.addRow();
  });

  print();
  print(table1.toString());
  print();
};
