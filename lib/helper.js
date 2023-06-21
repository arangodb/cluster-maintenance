/* jshint globalstrict:false, strict:false, sub: true */
/* global print, arango, db */
const fs = require('fs');
const _ = require('lodash');
const AsciiTable = require('./3rdParty/ascii-table');

// messages ///////////////////////////////////////////////////////////////////
const printGood = (msg) => {
  print('Good: ' + msg);
  print();
};

const printBad = (msg) => {
  // blink blink blink
  print('Bad: ' + msg);
  print();
};

const fatal = (msg) => {
  print("FATAL: " + msg);
  process.exit(1);
};

const padRight = function (value, maxLength) {
  if (value.length >= maxLength) {
    return value.substr(0, maxLength);
  }
  return value + Array(maxLength - value.length + 1).join(" ");
};
// messages - end /////////////////////////////////////////////////////////////

// connections ////////////////////////////////////////////////////////////////
let stringify;

const mustStringify = () => {
  if (stringify === undefined) {
    stringify = false;
    try {
      const ArangoError = require('@arangodb').ArangoError;
      try {
        arango.POST('/_api/agency/read', [["/"]]);
      } catch (err) {
        if (err instanceof ArangoError && err.errorNum === 10) {
          // bad parameter - wrong syntax
          stringify = true;
        }
      }
    } catch (err) {
    }
  }
  return stringify;
};

const httpWrapper = (method, url, payload, headers) => {
  if (!arango[method]) {
    fatal("Unknown HTTP method " + method);
  }
  if (method === 'GET' || method === 'DELETE' ||
      method === 'GET_RAW' || method === 'DELETE_RAW') {
    return arango[method](url, headers);
  }

  // whether or not HTTP request payload needs stringification (3.3) or not (3.4, 3.5)
  if (mustStringify()) {
    return arango[method](url, JSON.stringify(payload), headers);
  } else {
    return arango[method](url, payload, headers);
  }
};
// connections - end //////////////////////////////////////////////////////////

// roles //////////////////////////////////////////////////////////////////////
const getRole = () => {
  try {
    return db._version(true).details.role;
  } catch (ex) {
    fatal(ex + " -- Check if the endpoint is valid and authentication settings are correct.");
  }
};

const checkRole = (role_) => {
  const role = getRole();
  if (role === undefined) {
    if (role_ === "AGENT") {
      const url = '/_api/agency/read';
      const response = httpWrapper('POST_RAW', url, [["/"]]);

      if (response.code === 404 || response.code === 501) {
        return false;
      }

      return true;
    }

    if (role_ === "COORDINATOR") {
      const url = '/_api/cluster/endpoints';
      const response = httpWrapper('GET_RAW', url, [["/"]]);

      if (response.code === 403) {
        return false;
      }

      return true;
    }

    print("WARNING: unable to determine server role. " +
          "You can ignore this warning if the script is executed against a " +
          role_ + ".");
  } else if (role !== role_) {
    return false;
  }
  return true;
};

const checkCoordinator = () => {
  if (!checkRole("COORDINATOR")) {
    fatal("You need to connect to a coordinator, not a " + getRole());
  }
};

const checkAgent = () => {
  if (!checkRole("AGENT")) {
    fatal("Script needs a connection to an agent. Currently connected to a " +
          getRole() + ".");
  }
};

const checkLeader = () => {
  if (!checkRole("AGENT")) {
    fatal("Script needs a connection to the leader agent. Currently connected to a " +
          getRole() + ".");
  }

  // check for redirect
  const url = '/_api/agency/read';
  const redirect = httpWrapper('POST_RAW', url, [["/dummy"]]);

  if (redirect.code === 307) {
    const location = redirect.headers.location;
    fatal("You need to connect to the leader agent at '" +
          location.substr(0, location.length - url.length) + "'");
    } else if (redirect.error) {
      fatal("Got error while checking for leader agency: " +
      redirect.errorMessage);
  }

  // read agency from leader
  const response = httpWrapper('POST', url, [["/"]]);
  const stores = httpWrapper('GET', '/_api/agency/stores');
  return [response, stores];
};
// roles - end ////////////////////////////////////////////////////////////////

// file reading ///////////////////////////////////////////////////////////////
const readJsonFile = (file, mustBeRead = false) => {
  if (file === undefined || !fs.isFile(file)) {
    if (mustBeRead) {
      fatal("Can not read file: '" + file + "'");
    }
    return undefined;
  }
  print("reading file '" + file + "'");
  try {
    return JSON.parse(fs.read(file));
  } catch (e) {
    fatal(e);
  }
};
// file reading - end /////////////////////////////////////////////////////////

// agency dumps and config ////////////////////////////////////////////////////
const getAgencyDumpFromObject = (content) => {
  // content is a must be the parsed contnet of a json file
  if (!content) {
    return undefined;
  } else if (Array.isArray(content)) {
    return content[0];
  } else if (_.has(content, ".agency") && _.has(content, "arango")) {
    return content;
  } else {
    return content.agency;
  }
};

const getAgencyDumpFromObjectOrAgency = (obj = undefined) => {
  if (obj) {
    const agency = getAgencyDumpFromObject(obj);
    return [agency];
  } else {
    switchToAgencyLeader();
    const response = checkLeader();
    const agency = response[0][0];
    const stores = response[1];
    return [agency, stores];
  }
};

const getAgencyConfiguration = () => {
  if (!checkRole("AGENT")) {
    fatal("Script needs a connection to an agent. " +
          "Currently connected to a " + getRole() + ".");
  }
  const url = '/_api/agency/config';
  const response = httpWrapper('GET', url);
  return response;
};

const findAgencyFromCoordinator = () => {
  const response = httpWrapper('GET', '/_admin/cluster/health');

  if (response.code !== 200) {
    fatal("Cannot read '/_admin/cluster/health', got: ", JSON.stringify(response));
  }

  for (const key in response.Health) {
    const server = response.Health[key];

    if (server.Role === 'Agent' && server.Status === 'GOOD') {
      print("INFO found an agent at '" + server.Endpoint + "'");
      arango.reconnect(server.Endpoint, "_system");
      return;
    }
  }

  fatal("Cannot find an healthy agent");
};

const switchToAgencyLeader = () => {
  const url = '/_api/agency/read';

  if (getRole() === undefined) {
    // old version, do some test
    const response = httpWrapper('POST_RAW', url, [["/"]]);

    if (response.code === 404 || response.code === 501) {
      fatal("ERROR: need to be connected to an agent");
    }
  } else {
    if (checkRole("COORDINATOR")) {
      findAgencyFromCoordinator();
    }

    if (!checkRole("AGENT")) {
      fatal("Script needs a connection to an agent. " +
            "Currently connected to a " + getRole() + ".");
    }
  }

  let response = httpWrapper('POST_RAW', url, [["/"]]);
  if (response.code === 307) {
    const location = response.headers.location;
    const conn = location.substr(0, location.length - url.length);
    arango.reconnect(conn, "_system");

    if (!checkRole("AGENT")) {
      fatal("Reconnect did not connect to an agent. " +
            "Currently connected to a " + getRole() + ".");
    }

    response = httpWrapper('POST_RAW', url, [["/"]]);
    if (response.code !== 200) {
      fatal("Cannot switch to leader: " + response.errorMessage);
    }
  } else if (response.error) {
    fatal("Got error while checking for leader agency: " +
          response.errorMessage);
  }
};

const getAgencyHistoryFromCoordinator = () => {
  checkCoordinator();
  const response = httpWrapper('GET', "/_api/cluster/agency-dump");
  if (response.error) {
    if (response.code === 403 && response.errorNum === 11) {
      fatal("History is not supported by this version");
    }

    fatal("Got error while while getting agency history: " +
          response.errorMessage);
  }
  return response;
};
// agency dumps and config - end //////////////////////////////////////////////

// arguments and usage ////////////////////////////////////////////////////////
/*
 * Argument handling is very simple. Therefor we are a bit restricted in what is
 * possible:
 *
 * required arguments must be defined first:
 *
 *     command required1 required2 required3 ... optional1 optional2 optional3 ...
 *
 * The next restriction is that optional arguments can be only omitted at the tail:
 *
 *     command required1 required2 required3 ... optional1  -- good
 *     command required1 required2 required3 ... optional3  -- undefined behaviour
 *
 *
 * Sample definition:
 *
 *     Exports.args = [ { "name" : "input-file",    "optional" : false, "type": "jsonfile"},
 *                      { "name" : "target-server", "optional" : false, "type": "string"},
 *                      { "name" : "leader-cid",    "optional" : false, "type": "string"},
 *                      { "name" : "shared-index",  "optional" : false, "type": "string"},
 *                    ];
 *
 * Supported Types:
 *     - string
 *     - int
 *     - jsonfile (value will be the parsed object)
 *     - boolean
 *
 *  Usage in script:
 *
 *      let value = helper.getValue("valueName", args);
 *
 *  After this call `value` will contain the parsed and checked value or `undefined`.
 *
*/

const printUsage = (task) => {
  const usageArray = task.args;
  const itemToString = (usageObject) => {
    let out = "<" + usageObject.name + ">";
    if (usageObject.optional) {
      out = "[" + out + "]";
    }
    return out;
  };

  let arangosh = "";
  if (typeof task.args_arangosh === "string") {
    arangosh = task.args_arangosh;
  }

  print(usageArray.map((x) => itemToString(x))
                  .reduce((x, y) => { return x + " " + y; }, task.name) + " " + arangosh
       );
  usageArray.map((x) => {
    if (x.description !== undefined) {
      let delim = "-";
      if (x.optional === true) {
        delim = "(optional) -";
      }
      print(`  ${x.name} ${delim} ${x.description}`);
    }
  });
  print(task.info);
};

const countArgs = (usageArray) => {
  return usageArray.map((x) => x.optional)
    .reduce((acc, optional) => { return acc + ((optional === true) ? 0 : 1); }, 0);
};

const checkArgs = (task, args) => {
  args.shift(); // remove task name from args
  
  const proArgs = args.length;
  const reqArgs = countArgs(task.args);

  if (proArgs < reqArgs) {
    printUsage(task);
    fatal("Not enough arguments - " + proArgs +
          " arguments provided while " + reqArgs +
          " required!");
  }

  for (let i = 0; i < proArgs; i++) {
    const given = args[i];

    if (i >= task.args.length) {
      // too many arguments
      print("ignoring superfluous argument #" + i + "': " + given + "'");
      continue;
    }
    const toSet = task.args[i];
    try {
      switch (toSet.type) {
        case 'string':
          toSet.value = given;
          break;
        case 'jsonfile':
          toSet.value = readJsonFile(given, true /* must read */);
          break;
        case 'boolean':
        case 'bool': {
          const v = given.toLowerCase();
          if (v === 'true' || v === '1' || v === 'y') {
            toSet.value = true;
          } else {
            toSet.value = false;
          }
        }
          break;
        case 'int':
          toSet.value = Number.parseInt(given);
          break;
        default:
          fatal("Unknown argument type: " + toSet.type);
      }
    } catch (ex) {
      fatal("Error while parsing value for argument '" + task.name + "' message: " + ex);
    }
  }

  return task.args;
};

const checkGlobalArgs = (args) => {
  const options = {};

  let l = args.length;

  for (let i = 1; i < l; i++) {
    const name = args[i];
    let found = 0;

    if (name === '--find-leader') {
      print("INFO: `--find-leader` is no longer necessary and is ignored");
      found = 1;
    } else if (name === '--force') {
      options.force = true;
      found = 1;
    } else if (name === '--ignore-versions' || name === '--ignore-version') {
      options.ignoreVersion = true;
      found = 1;
    }

    if (found > 0) {
      args.splice(i, found);
      l -= found;
    }
  }

  return options;
};

const getValue = (name, args) => {
  // handle default values here?
  const rv = args.find(x => x.name === name);
  if (rv === undefined) {
    fatal("Trying to access undefined argument: '" + name + "'");
  }
  return rv.value;
};
// arguments and usage - end //////////////////////////////////////////////////

// other helpers - begin //////////////////////////////////////////////////////
const extractDatabases = function (info, dump) {
  const databases = {};

  _.each(dump.arango.Plan.Databases, function (database, name) {
    databases[name] = _.extend({
      collections: [],
      shards: [],
      leaders: [],
      followers: [],
      realLeaders: [],
      isSystem: (name.charAt(0) === '_'),
      data: database
    }, database);
  });

  info.databases = databases;
  info.collections = {};
  info.shardsPrimary = {};
  info.zombies = [];
  info.broken = [];
  info.obsoleteCollections = [];

  const allCollections = dump.arango.Plan.Collections;

  _.each(allCollections, function (collections, dbName) {
    let database = databases[dbName];
    if (database === undefined) {
      print("Attention: Database with name'", dbName, "' is not in Databases");
      database = {
        collections:[],
        shards: [],
        leaders: [],
        followers: [],
        realLeaders: [],
        isSystem: (dbName.charAt(0) === '_'),
        data: {}
      };
    }

    _.each(collections, function (collection, cId) {
      if (collection.name === undefined && collection.id === undefined) {
        info.zombies.push({
          database: dbName,
          cid: cId,
          data: collection
        });
      } else if (collection.name === undefined || collection.id === undefined) {
        info.broken.push({
          database: dbName,
          cid: cId,
          collection: collection,
          data: collection
        });
      } else {
        const full = dbName + "/" + collection.name;
        const coll = {
          name: collection.name,
          fullName: full,
          distributeShardsLike: collection.distributeShardsLike || '',
          numberOfShards: collection.numberOfShards,
          replicationFactor: collection.replicationFactor,
          isSmart: collection.isSmart,
          type: collection.type,
          id: cId
        };

        if (collection.writeConcern) {
          coll.writeConcern = collection.writeConcern;
        } else {
          coll.writeConcern = 1;
        }

        database.collections.push(coll);

        if (_.has(info.collections, full)) {
          info.obsoleteCollections.push(full);

          if (info.collections[full].id > coll.id) {
            info.collections[full] = coll;
          }
        } else {
          info.collections[full] = coll;
        }

        coll.shards = [];
        coll.leaders = [];
        coll.followers = [];

        _.each(collection.shards, function (shard, sName) {
          coll.shards.push(shard);

          const s = {
            shard: sName,
            database: dbName,
            collection: collection.name
          };

          if (shard.length > 0) {
            coll.leaders.push(shard[0]);
            setGlobalShard(info,
              _.extend({
                dbServer: shard[0],
                isLeader: true,
                isReadLeader: (coll.distributeShardsLike === '')
              }, s));

            for (let i = 1; i < shard.length; ++i) {
              coll.followers.push(shard[i]);
              setGlobalShard(info,
                _.extend({
                  dbServer: shard[i],
                  isLeader: false
                }, s));
            }
          }
        });

        if (coll.distributeShardsLike !== '') {
          coll.realLeaders = [];
        } else {
          coll.realLeaders = coll.leaders;
        }

        database.shards = database.shards.concat(coll.shards);
        database.leaders = database.leaders.concat(coll.leaders);
        database.followers = database.followers.concat(coll.followers);
        database.realLeaders = database.realLeaders.concat(coll.realLeaders);
      }
    });
  });
};

const extractPrimaries = function (info, dump) {
  const primariesAll = {};
  const primaries = {};

  const health = dump.arango.Supervision.Health;

  _.each(health, function (server, key) {
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

const setGlobalShard = function (info, shard) {
  const dbServer = shard.dbServer;
  const isLeader = shard.isLeader;

  if (!info.shardsPrimary[dbServer]) {
    info.shardsPrimary[dbServer] = {
      leaders: [],
      followers: [],
      realLeaders: []
    };
  }

  if (isLeader) {
    info.shardsPrimary[dbServer].leaders.push(shard);

    if (shard.isReadLeader) {
      info.shardsPrimary[dbServer].realLeaders.push(shard);
    }
  } else {
    info.shardsPrimary[dbServer].followers.push(shard);
  }
};

const findServer = function (dump, name) {
  const health = dump.arango.Supervision.Health;

  if (_.has(health, name)) {
    return {serverId: name, shortName: health[name].ShortName};
  }

  let serverId = "";
  let shortName = "";

  _.each(health, function (server, key) {
    if (server.ShortName === name) {
      serverId = key;
      shortName = name;
    }
  });

  return {serverId, shortName};
};

const showServers = function (dump, agency) {
  const servers = {};

  if (dump) {
    const health = dump.arango.Supervision.Health;

    _.each(health, function (server, key) {
      let status = server.Status;

      if (_.has(dump.arango.Target.FailedServers, key)) {
        status = 'FAILED';
      } else if (_.includes(dump.arango.Target.CleanedServers, key)) {
        status = 'CLEANED';
      }

      servers[key] = {
        id: key,
        endpoint: server.Endpoint,
        status: status,
        shortName: server.ShortName
      };
    });
  }

  if (agency) {
    const pool = agency.configuration.pool;
    const active = agency.configuration.active;

    _.each(pool, function (endpoint, key) {
      servers[key] = {
        id: key,
        endpoint: endpoint
      };

      if (key === agency.leaderId) {
        servers[key].status = 'LEADER';
      } else if (_.includes(active, key)) {
        servers[key].status = 'FOLLOWER';
      } else {
        servers[key].status = 'POOL';
      }
    });
  }

  const table = new AsciiTable('Servers');
  table.setHeading('ID', 'Address', 'Short Name', 'Status');

  _.each(_.sortBy(_.keys(servers)), function (key) {
    const server = servers[key];
    table.addRow(server.id, server.endpoint, server.shortName, server.status);
  });

  print();
  print(table.toString());
  print();
};
// other helpers - end ////////////////////////////////////////////////////////

// messages
exports.printGood = printGood;
exports.printBad = printBad;
exports.fatal = fatal;
exports.padRight = padRight;

// sharding information
exports.extractPrimaries = extractPrimaries;
exports.extractDatabases = extractDatabases;

exports.showServers = showServers;
exports.findServer = findServer;

// connections
exports.httpWrapper = httpWrapper;

// roles
exports.checkCoordinator = checkCoordinator;
exports.getRole = getRole;
exports.checkLeader = checkLeader;
exports.checkAgent = checkAgent;

// file reading
exports.readJsonFile = readJsonFile;

// agency dump and config
exports.getAgencyDumpFromObjectOrAgency = getAgencyDumpFromObjectOrAgency;
exports.getAgencyHistoryFromCoordinator = getAgencyHistoryFromCoordinator;
exports.getAgencyConfiguration = getAgencyConfiguration;
exports.switchToAgencyLeader = switchToAgencyLeader;

// arguments and usage
exports.printUsage = printUsage;
exports.checkArgs = checkArgs;
exports.checkGlobalArgs = checkGlobalArgs;
exports.getValue = getValue;
