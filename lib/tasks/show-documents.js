/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */
exports.name = "show-documents";
exports.group = "analyze tasks";
exports.args = [
  {
    "name": "level",
    "optional": false,
    "type": "string",
    "description": "'collection' or 'database'"
  },
  {
    "name": "type",
    "optional": false,
    "type": "string",
    "description": "'count', 'indexes', 'size', 'total'"
  }
];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Shows number of documents.";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.12.99";
exports.info = `
Shows the number of documents in all collections in all databases.
`;

exports.run = function (extra, args) {
  const helper = require('../helper.js');
  const printBad = helper.printBad;

  // at what level shall we disply the information
  let level = helper.getValue("level", args);

  if (level === "collection" || level === "col") {
    level = "col";
  } else if (level === "database" || level === "db") {
    level = "db";
  } else {
    helper.fatal("argument 'level', expecting either 'collection' or 'database', got '" +
                 level + "'");
  }

  // at what level shall we disply the information
  const type = helper.getValue("type", args);

  if (type !== 'count' && type !== 'size' &&
      type !== 'indexes' && type !== 'total') {
    helper.fatal("argument 'type', expecting either 'count', 'indexes', 'size', " +
                 "'total' got '" + type + "'");
  }

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

  helper.showServers(conf, helper.getAgencyConfiguration());

  _.each(conf.arango.Current.Collections, function (val, dbname) {
    info[dbname] = {};

    _.each(val, function (val, cid) {
      if (!shardMap.hasOwnProperty(dbname)) {
        printBad("database '" + dbname + "' is in current, but not in plan");
        return;
      }

      let d1 = shardMap[dbname];

      if (!d1.hasOwnProperty(cid)) {
        printBad("collection '" + cid + "' in database '" +
                 dbname + "' is in current, but not in plan");
        return;
      }

      let d2 = d1[cid];

      if (!d2.hasOwnProperty('name')) {
        printBad("collection '" + cid + "' in database '" +
                 dbname + "' in plan has no name");
      }

      const cname = d2.name;
      info[dbname][cname] = {};

      _.each(val, function (val, shard) {
        const servers = val.servers;
        info[dbname][cname][shard] = [];

        _.each(servers, function (server) {
          const s = health[server];
          if (s) {
            const status = s.Status;
            const ip = s.Endpoint;
            const shortName = s.ShortName;
            const sinfo = {server, status, ip, dbname, cid, cname, shard, shortName};
            info[dbname][cname][shard].push(sinfo);

            if (!serverMap.hasOwnProperty(server)) {
              serverMap[server] = [];
              serverCount[server] = { num: ++scount, shortName, ip };
            }

            serverMap[server].push(sinfo);
          } else {
            print("INFO ignoring server '" + server + "'");
          }
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

      if (collection) {
        if (type === 'count') {
          const count = collection.count();
          entry.count = count;
        } else if (type === 'size') {
          const count = collection.figures().documentsSize;
          entry.count = count;
        } else if (type === 'indexes') {
          const count = collection.figures().indexes.size;
          entry.count = count;
        } else if (type === 'total') {
          const count = collection.figures().documentsSize + collection.figures().indexes.size;
          entry.count = count;
        }
      }
    });
  });

  const keys = _.sortBy(_.keys(info));

  const table1 = new AsciiTable('Leader and Follower');
  const header = ['database', 'collection', 'shard', type];
  const offset = header.length;

  _.each(serverCount, function (val, id) {
    header[offset + val.num - 1] = val.shortName;
  });

  table1.setHeading(header);

  let countTotal = (new Array(scount)).fill(0);

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
          countServers[serverCount[name].num] += count;
        });

        rows2.push(_.concat([dbname, cname, sname], countServers));

        countCollection = _.zipWith(countCollection, countServers, (a, b) => a + b);
      });

      rows1.push(_.concat([dbname, cname, ''], countCollection));
      _.each(rows2, (row) => rows1.push(row));

      countDatabase = _.zipWith(countDatabase, countCollection, (a, b) => a + b);
    });

    table1.addRow(_.concat([dbname, '', ''], countDatabase));
    countTotal = _.zipWith(countTotal, countDatabase, (a, b) => a + b);

    if (level === 'col') {
      _.each(rows1, (row) => {
        table1.addRow(row);
      });

      table1.addRow();
    }
  });

  table1.addRow();
  table1.addRow(_.concat(["TOTAL", '', ''], countTotal));

  print();
  print(table1.toString());
  print();
};
