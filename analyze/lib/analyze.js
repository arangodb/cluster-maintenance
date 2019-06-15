// imports
// const agency = require('agency');
const fs = require('fs');
const _ = require('underscore');
const AsciiTable = require('./ascii-table');

// variables
// const dump = agency.dump();

const dump = JSON.parse(fs.read('dump.json')).agency;

let extractPrimaries = function(info, dump) {
  let primariesAll = {};
  let primaries = {};

  const health = dump.arango.Supervision.Health;

  _.each(health, function(server, key) {
    if (key.substring(0, 4) === 'PRMR') {
      primariesAll[key] = server;

      if (server.Status === 'GOOD') {
        primaries[key] = server;
      }
    }
  });

  info.primaries = primaries;
  info.primariesAll = primariesAll;
};

let printPrimaries = function(info) {
  var table = new AsciiTable('Primaries');
  table.setHeading('', 'status');

  _.each(info.primariesAll, function(server, name) {
    table.addRow(name, server.Status);
  });

  print(table.toString());
};

let extractDatabases = function(info, dump) {
  let databases = {};

  _.each(dump.arango.Plan.Databases, function(database, name) {
    databases[name] = _.extend({
      isSystem: (name.charAt(0) === '_')
    }, database);
  });

  info.databases = databases;
  info.collections = {};

  let allCollections = dump.arango.Plan.Collections;

  _.each(allCollections, function(collections, dbName) {
    let database = databases[dbName];
    database.collections = [];
    database.shards = [];

    _.each(collections, function(collection, cId) {
      let full = dbName + "/" + collection.name;
      let coll = {
        name: collection.name,
        fullName: full,
        distributeShardsLike: collection.distributeShardsLike,
        numberOfShards: collection.numberOfShards,
        isSmart: collection.isSmart,
        type: collection.type
      };

      database.collections.push(coll);
      info.collections[full] = coll;

      coll.shards = [];

      _.each(collection.shards, function(shard, sName) {
        coll.shards.push(shard);
      });

      database.shards = database.shards.concat(coll.shards);
    });
  });
};

let printDatabases = function(info) {
  var table = new AsciiTable('Databases');
  table.setHeading('', 'collections', 'shards');

  _.each(_.sortBy(info.databases, x => x.name), function(database, name) {
    table.addRow(database.name, database.collections.length, database.shards.length);
  });

  print(table.toString());
};

let printCollections = function(info) {
  var table = new AsciiTable('collections');
  table.setHeading('', 'Shards Like', 'Shards', 'Type', 'Smart');

  _.each(_.sortBy(info.collections, x => x.fullName), function(collection, name) {
    table.addRow(collection.fullName, collection.distributeShardsLike,
                 collection.numberOfShards, collection.type,
                 collection.isSmart);
  });

  print(table.toString());
};

const info = {};

extractPrimaries(info, dump);
printPrimaries(info);
print();

extractDatabases(info, dump);
printDatabases(info);
print();
printCollections(info);
