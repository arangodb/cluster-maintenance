/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango */
exports.name = "collect-db-info";
exports.group= "standalone tasks";
exports.args = [ 
  { "name" : "output-file", "optional" : false, "type": "string"},
];
exports.args_arangosh = " --server.endpoint SINGLESERVER-OR-COORDINATOR --server.database DATABASE";
exports.description = "Dumps information about the database and collection.";
exports.selfTests = ["arango", "db"];
exports.requires = "3.3.23 - 3.5.99";
exports.info = `
Dumps information about the database and collection.
`;

const _ = require("lodash");

let uniqueValue = 0;

const anonymize = function (doc) {
  if (Array.isArray(doc)) {
    return doc.map(anonymize);
  }
  if (typeof doc === 'string') {
    // make unique values because of unique indexes
    return Array(doc.length + 1).join('X') + uniqueValue++;
  }
  if (doc === null || typeof doc === 'number' || typeof doc === 'boolean') {
    return doc;
  }
  if (typeof doc === 'object') {
    let result = {};
    Object.keys(doc).forEach(function (key) {
      if (key.startsWith('_') || key.startsWith('@')) {
        // This excludes system attributes in examples
        // and collections in bindVars
        result[key] = doc[key];
      } else {
        result[key] = anonymize(doc[key]);
      }
    });
    return result;
  }
  return doc;
};

const processCollection = function(collection) {
  let name = collection._name;
  let type = collection._type;
  let info = {
    name: name,
    id: collection._id,
    status: collection._status,
    type: type,
    count: collection.count(true),
    figures: collection.figures(),
    indexes: collection.getIndexes(),
    properties: collection.properties(),
    revision: collection.revision()
  };

  if (name === '_graphs' || name === '_aqlfunctions' || name === '_analyzers') {
    info.documents = collection.toArray();
  } else if (name[0] != '_') {
    let max = 10;
    let examples = db._query(`
      FOR doc IN @@collection
        LIMIT @max
        RETURN doc
      `, { max, "@collection": name }).toArray();
    info.examples = examples.map(anonymize);
  }

  return info;
};

const analyzeEdgeCollection = function(collection) {
  let name = collection._name;
  let max = 1000;
  let examples = db._query(`
    FOR doc IN @@collection
      LIMIT @max
      RETURN { from: doc._from, to: doc._to }
    `, { max, "@collection": name }).toArray();
  let count = examples.length;

  let from = _.map(examples, x => x.from.split('/')[0]);
  let to = _.map(examples, x => x.to.split('/')[0]);
  let fromto = _.map(examples, x => [ x.from.split('/')[0], x.to.split('/')[0] ]);

  return {
    edge: name,
      fromDistribution: _.mapValues(_.groupBy(from), x => x.length / count),
      toDistribution: _.mapValues(_.groupBy(to), x => x.length / count),
      distribution: _.mapValues(_.groupBy(fromto), x => x.length / count)
  };
};

exports.run = function(extra, args) {
  // imports
  const fs = require('fs');
  const helper = require('../helper.js');

  try {
    let file = helper.getValue("output-file", args);
    let info = {};

    info.db = {
      version: db._version(true),
      role: helper.getRole(),
      name: db._name(),
      engine: db._engine(),
      stats: db._engineStats()
    };

    info.collections = [];

    let collections = db._collections();
    for (const c of collections) {
      print("processing collection '" + c._name + "'");
      info.collections.push(processCollection(c));
    }

    info.edges = [];

    for (const c of collections) {
      if (c._type === 3 && 0 < c.count()) {
	print("analyizing edge collection '" + c._name + "'");
	info.edges.push(analyzeEdgeCollection(c));
      }
    }

    fs.write(file, JSON.stringify(info));

    helper.printGood("wrote info to: " + file)
  } catch (ex) {
    helper.fatal("cannot get information: " + ex)
  }

};
