/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */
exports.name = "show-dbstats";
exports.group = "analyze tasks";
exports.args = [];
exports.args_arangosh = " --server.endpoint AGENT-OR-COORDINATOR";
exports.description = "Shows statistics from database serversr";
exports.selfTests = ["arango", "db", "leaderAgencyConnection"];
exports.requires = "3.3.23 - 3.8.99";
exports.info = `
Shows all DB statictics from DBservers.
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
  const serverMap = {};

  _.each(conf.arango.Current.Collections, function (val, dbname) {
    _.each(val, function (val, cid) {
      const c = shardMap[dbname][cid];
      if (c) {
        const cname = c.name;
        _.each(val, function (val, shard) {
          const servers = val.servers;
          _.each(servers, function (server) {
            const s = health[server];
            const status = s.Status;
            const ip = s.Endpoint;

            serverMap[server] = {
              server, status, ip, dbname, cid, cname, shard
            };
          });
        });
      } else {
        print("INFO ignoring unplanned collection '" + dbname + "/" + cid + "'");
      }
    });
  });

  _.each(serverMap, function (val, id) {
    const ip = val.ip;

    print("================================================================================");
    print(id + " at '" + ip + "'");
    print("================================================================================");

    arango.reconnect(ip, "_system");

    const stats = db._engineStats();

    const table = new AsciiTable('Leader and Follower');
    table.setHeading('Metric', 'Value');

    _.each([
      "rocksdb.num-files-at-level0",
      "rocksdb.compression-ratio-at-level0",
      "rocksdb.num-files-at-level1",
      "rocksdb.compression-ratio-at-level1",
      "rocksdb.num-files-at-level2",
      "rocksdb.compression-ratio-at-level2",
      "rocksdb.num-files-at-level3",
      "rocksdb.compression-ratio-at-level3",
      "rocksdb.num-files-at-level4",
      "rocksdb.compression-ratio-at-level4",
      "rocksdb.num-files-at-level5",
      "rocksdb.compression-ratio-at-level5",
      "rocksdb.num-files-at-level6",
      "rocksdb.compression-ratio-at-level6",
      "rocksdb.num-immutable-mem-table",
      "rocksdb.num-immutable-mem-table-flushed",
      "rocksdb.mem-table-flush-pending",
      "rocksdb.compaction-pending",
      "rocksdb.background-errors",
      "rocksdb.cur-size-active-mem-table",
      "rocksdb.cur-size-all-mem-tables",
      "rocksdb.size-all-mem-tables",
      "rocksdb.num-entries-active-mem-table",
      "rocksdb.num-entries-imm-mem-tables",
      "rocksdb.num-deletes-active-mem-table",
      "rocksdb.num-deletes-imm-mem-tables",
      "rocksdb.estimate-num-keys",
      "rocksdb.estimate-table-readers-mem",
      "rocksdb.num-snapshots",
      "rocksdb.oldest-snapshot-time",
      "rocksdb.num-live-versions",
      "rocksdb.min-log-number-to-keep",
      "rocksdb.estimate-live-data-size",
      "rocksdb.live-sst-files-size"
    ], function (key) {
      table.addRow(key, stats[key]);
    });

    print();
    print(table.toString());
    print();

    print(stats["rocksdb.dbstats"]);

    const cf = stats.columnFamilies;

    _.each(cf, function (stat, family) {
      print(stat.dbstats);
    });

    print();
  });
};
