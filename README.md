# ArangoDB Debug Scripts

**These scripts are to be used with caution, under the guidance of ArangoDB support.**

## Purpose

A collection of several helper scripts that gather diagnostic information about an
ArangoDB cluster and also optionally apply modifications to it.

## Prerequisites

In order to use these scripts, a working installation of the ArangoShell (arangosh)
is needed. The scripts currently support ArangoDB versions 3.3, 3.4, 3.5 and 3.6.

## Installation

Just clone this repository to some location on your local filesystem, then `cd`
into the directory. Please note that some of the debug scripts may want to write into
that directory too.

## Usage

If `arangosh` is installed and is found in your `$PATH` you should be able to just run:
```
./debugging/index.js help
```
otherwise you need to use:
```
./path/to/arangosh --javascript.execute ./debugging/index.js help
```

If any of the commands fails with `cannot find configuration file`, you will additionally
have to supply the path to a valid arangosh.conf configuration file via the `-c` option:

```
./path/to/arangosh -c /etc/arangodb3/arangosh.conf --javascript.execute ./debugging/index.js help
```
(here it is expected that the configuration file resides in `/etc/arangodb3`, please adjust
as required).

One of the above commands should provide you with a list of the available tasks. Each task
will be followed by a range of suitable ArangoDB versions and a short description of the
task. For this instruction, we will always use the short version of the command.

```
» ./debugging/index.js help
Please specify a password:

General Usage: ./debugging/index.js <taskname> [parameters]
   Help Usage: ./debugging/index.js help <taskname>

Post an agency plan to a new leader agency. Only for debug purpose! DO NOT USE IN PRODUCTION!:
  post-agency-plan                     3.3.23 - 3.5.99    Posts an agency dump to an ArangoDB agency leader.

analyze tasks:
  analyze                              3.3.23 - 3.5.99    Performs health analysis on your cluster and produces input files for other cleanup tasks.
  show-supervision                     3.3.23 - 3.6.99    Show the state of the supervision.

cleanup tasks:
  clear-maintenance                    3.3.23 - 3.6.99    Clear maintenance and hot-backup flag.
  create-missing-collections           3.3.23 - 3.5.99    Adds missing collections found by the analyze task.
  create-missing-system-collections    3.3.23 - 3.5.99    Adds missing system collections for all databases (does not require the analyze task).
  remove-cleaned-failovers             3.3.23 - 3.5.99    Clears cleaned failover candidates found by analyze task.
  remove-dead-primaries                3.3.23 - 3.5.99    Removes dead primaries found by analyze task.
  remove-skeleton-databases            3.3.23 - 3.5.99    Removes skeleton databases found by analyze task.
  remove-zombie-callbacks              3.3.23 - 3.5.99    Removes zombie callbacks found by analyze task.
  remove-zombie-coordinators           3.3.23 - 3.5.99    Removes dead coordinators found by analyze task.
  remove-zombies                       3.3.23 - 3.5.99    Removes zombie collections found by analyze task.

move shard tasks:
  create-move-analysis                 3.3.23 - 3.5.99    Creates analysis for a plan to rebalance shards in your cluster.
  create-move-plan (deprecated)        3.3.23 - 3.5.99    Creates plan to rebalance shards in your cluster.
  execute-move-plan                    3.3.23 - 3.5.99    Executes plan created by create-move-plan task.
  force-failover                       3.3.23 - 3.5.99    Performs forced failover as calculated by analyze task.
  show-move-shards                     3.3.23 - 3.5.99    Allows to inspect shards being moved.

standalone tasks:
  collect-db-info                      3.3.23 - 3.5.99    Dumps information about the database and collection.
  dump                                 3.3.23 - 3.5.99    Dumps the agency.
  help                                 3.3.23 - 4.0.0     Shows this help.
  users-permissions                    3.3.23 - 3.5.99    Extracts all users and permissions from the system database.
```

Please note that only those tasks are shown that are supported by the version of
ArangoDB actually in use.

Please also note that the ArangoShell may ask you for a password. For getting help
on the available tasks you can simply ignore the password prompt. For invoking any of
the "real" tasks later, please keep in mind that the tasks are executed in a regular
ArangoShell, so it supports the options `--server.endpoint` to connect to an
arbitrary server, `--server.username` to specify the database user and also
`--server.ask-jwt-secret` for passing credentials.
Additionally, please note that some tasks require a working connection to either
a coordinator or the leader agent in the cluster. This connection can also be
established by using ArangoShell's parameter `--server.endpoint`.

If you pass a task name after `help` you will get additional information on the
task and how to invoke it, for example:

```
» ./debugging/index.js help create-missing-system-collections
Please specify a password:
Usage for task: create-missing-system-collections

create-missing-system-collections --server.endpoint COORDINATOR

Helper script to create missing system collections. It will iterate over the list
of databases and check for the availability of the default system collections
in them. Will create the missing system collections automatically.

To be used from the arangosh, with a privileged user (i.e. a user that
has write privileges for all databases).
```

## Authentication and SSL

As shown by its detailed help, the task `create-missing-system-collections` needs a
connection to a cluster coordinator using a privileged user. To invoke it using a
connection to a specific endpoint and using a specific database user, use `--server.endpoint`
and `--server.username`, e.g.:
```
» ./debugging/index.js --server.endpoint tcp://domain:port --server.username admin create-missing-system-collections
```

If connecting to an SSL enabled server you need to use `ssl` instead of `tcp` when
specifying the server endpoint.

In order to make use of JWT tokens please add `--server.ask-jwt-secret` to the command-line.

You can get your JWT in a kubernetes cluster with a command similar to the following:
`kubectl get secrets nameofyourdep-jwt -o json | jq -r '.data.token' | base64 -D`

## Known Issues

Please heed warnings about undetermined server roles in older versions, namely in the
ArangoDB 3.3 series and ArangoDB before 3.4.6.
There we can not tell the server roles for sure so please check the
given endpoints twice.

## Creating an Agency Dump

In alternative to connecting to a leader agent, some of the scripts will accept an agency dump JSON
file as their input.

In order to create an agency dump from scratch, one can use the `curl` tool (not bundled
with ArangoDB) to connect to an agent and retrieve the dump.
An example command for this is:

```
» curl -L -d '[["/"]]' -o dump.json http://AGENCY_SERVER:AGENCY_PORT/_api/agency/read
```

This will use *curl* to get the dump file from an agent. You need to specify on
agency endpoint. In case this is not the leader *curl* will follow the redirects
which will point to the leader.

You might need the JWT token: see all details on how to generate an Agency dump at
https://www.arangodb.com/docs/3.4/troubleshooting-cluster-agency-dump.html

Alternatively one can run the `dump` task against the cluster's leader agent, e.g.:
```
» ./debugging/index.js dump --server.endpoint AGENT
```

**Important:** do not use the coordinator route to create the agency dump: create the dump
connecting directly to the agency.
