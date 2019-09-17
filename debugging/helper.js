/*jshint globalstrict:false, strict:false, sub: true */
/*global ARGUMENTS, print, arango, db */
const fs = require('fs');

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

const padRight = function(value, maxLength) {
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
      let ArangoError = require('@arangodb').ArangoError;
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
  if (method === 'GET' || method === 'DELETE') {
    return arango[method](url, headers);
  }

  // whether or not HTTP request payload needs stringification (3.3) or not (3.4, 3.5)
  if (mustStringify()) {
    return arango[method](url, JSON.stringify(payload), headers);
  } else {
    return arango[method](url, payload, headers);
  }
};
// connections - end///////////////////////////////////////////////////////////


// roles //////////////////////////////////////////////////////////////////////
const getRole = () => {
  try {
    return db._version(true).details.role;
  } catch (ex) {
    fatal(ex + " -- Check if the endpoint is valid and authentication settings are correct.");
  }
};

const checkRole = (role_) => {
  let role = getRole();
  if (role === undefined) {
    print("WARNING: unable to determine server role. You can ignore this warning if the script is executed against a " + role_ + ".");
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
    fatal("Script needs a connection to the leader agent. Currently connected to a " + getRole() + ".");
  }
};

const checkLeader = () => {
  if (!checkRole("AGENT")) {
    fatal("Script needs a connection to the leader agent. Currently connected to a " + getRole() + ".");
  }
  const response = httpWrapper('POST', '/_api/agency/read', [["/"]]); // read agency form leader
  if (response.code === 307) {
    fatal("You need to connect to the leader agent");
  } else if (response.error) {
    fatal("Got error while checking for leader agency: " + response.errorMessage);
  }
  return response;
};
// roles - end ////////////////////////////////////////////////////////////////


// file reading ///////////////////////////////////////////////////////////////
const readJsonFile = (file, mustBeRead = false) => {
  if(file === undefined || !fs.isFile(file)){
    if(mustBeRead) {
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


// agency dumps ///////////////////////////////////////////////////////////////
const getAgencyDumpFromObject = (content) => {
  // content is a must be the parsed contnet of a json file
  if (content === undefined) {
    return undefined;
  }
  if (Array.isArray(content)) {
    return content[0];
  } else {
    return content.agency;
  }
};

const getAgencyDumpFromObjectOrAgency = (obj = undefined) => {
  let agency = getAgencyDumpFromObject(obj);
  if(agency === undefined) {
    const response = checkLeader();
    agency = response[0];
  }
  return agency;
};
// agency dumps - end /////////////////////////////////////////////////////////


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
 *
 *  Usage in script:
 *
 *      let value = helper.getValue("valueName", args);
 *
 *  After this call `value` will contain the parsed and checked value or `undefined`.
 *
*/

const printUsage = (task) => {
  let usageArray = task.args;
  let itemToString = (usageObject) => {
    let out = "<" + usageObject.name + ">";
    if(usageObject.optional) {
      out = "[" + out + "]";
    }
    return out;
  };

  let arangosh = "";
  if(typeof task.args_arangosh === "string") {
    arangosh = task.args_arangosh;
  }

  print(usageArray.map((x) => itemToString(x))
                  .reduce((x, y) => { return x + " " + y; }, task.name) + " " + arangosh
       );
  usageArray.map((x) => {
    if(x.description !== undefined) {
      let delim = "-";
      if(x.optional === true) {
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

  if(proArgs < reqArgs) {
    printUsage(task);
    fatal("Not enough arguments - "
          + proArgs  + " arguments provided while "
          + reqArgs + " required!");
  }

  for(let i = 0; i < proArgs; i++) {
    let given = args[i];
    let toSet = task.args[i];
    try {
      switch(toSet.type) {
      case 'string':
        toSet.value = given;
        break;
      case 'jsonfile':
        toSet.value = readJsonFile(given, true /*must read*/);
        break;
      case 'int':
        toSet.value = Number.parseInt(given);
        break;
      default:
        fatal("Unknown argument type: " + toSet.type);
      }
    } catch(ex){
      fatal("Error while parsing value for argument '" + task.name + "' message: " + ex);
    }
  }

  return task.args;
};

const getValue = (name, args) => {
  //handle defualt vaules here?
  const rv = args.find( x  => x.name === name );
  if( rv === undefined) {
      fatal("Trying to access undefined argument: '" + name + "'");
  };
  return rv.value;
};

const getType = (name, args) => {
  const rv = args.find( x  => x.name === name );
  if( rv === undefined) { return rv; };
  return rv.type;
};
// arguments and usage - end //////////////////////////////////////////////////


// messages
exports.printGood = printGood;
exports.printBad = printBad;
exports.fatal = fatal;
exports.padRight = padRight;

// connections
exports.httpWrapper = httpWrapper;

// roles
exports.checkCoordinator = checkCoordinator;
exports.checkLeader = checkLeader;

// file reading
exports.readJsonFile = readJsonFile;
// agency dump
exports.getAgencyDumpFromObjectOrAgency = getAgencyDumpFromObjectOrAgency;

// arguments and usage
exports.printUsage = printUsage;
exports.checkArgs = checkArgs;
exports.getValue = getValue;
