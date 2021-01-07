# ArangoDB Cluster Maintenance Tools

**These scripts are to be used with caution, under the guidance of ArangoDB support.**

## Purpose

A collection of several tools that gather diagnostic information
about an ArangoDB cluster and also optionally apply modifications
to it.

## Prerequisites

In order to use these scripts, a working installation of the
ArangoShell (arangosh) is needed. The scripts currently support
ArangoDB versions 3.3, 3.4, 3.5, 3.6, and 3.7.

Please note, that some tasks are only available in certain
ArangoDB versions. *help* will show which task is supported
by which version.

## Installation

Just clone this repository to some location on your local filesystem,
then `cd` into the directory. Please note that some of the maintenance
tools may want to write into that directory too.

## Usage

If `arangosh` is installed and is found in your `$PATH` you should be
able to just run:

```
./maintenance.sh help
```

otherwise you should set the environment variable `ARANGOSH`:

```
ARANGOSH=./path/to/arangosh ./maintenance.sh help
```

Note that you might be asked for a password. For help, just press
return. See below for more details.

One of the above commands should provide you with a list of the
available tasks. Each task will be followed by a range of suitable
ArangoDB versions and a short description of the task. For this
instruction, we will always use the short version of the command.

```
> ./maintenance.sh help
Please specify a password:

General Usage: ./maintenance.sh <taskname> [parameters]
   Help Usage: ./maintenance.sh help <taskname>

Post an agency plan to a new leader agency. Only for debug purpose! DO NOT USE IN PRODUCTION!:
  post-agency-plan                     3.3.23 - 3.6.99    Posts an agency dump to an ArangoDB agency leader.

analyze tasks:
  analyze                              3.3.23 - 3.6.99    Performs health analysis on your cluster and p  ...

move shard tasks:
  create-move-analysis                 3.3.23 - 3.6.99    Creates analysis for a plan to rebalance shards in your cluster.
  ...
```

Please note that only those tasks are shown that are supported by the
version of ArangoDB actually in use.

Please also note that the ArangoShell may ask you for a password. For
getting help on the available tasks you can simply ignore the password
prompt. For invoking any of the "real" tasks later, please keep in
mind that the tasks are executed in a regular ArangoShell, so it
supports the options `--server.endpoint` to connect to an arbitrary
server, `--server.username` to specify the database user and also
`--server.ask-jwt-secret` for passing credentials.

Additionally, please note that some tasks require a working connection
to either a coordinator or the leader agent in the cluster. This
connection can also be established by using ArangoShell's parameter
`--server.endpoint`.

If you pass a task name after `help` you will get additional
information on the task and how to invoke it, for example:

```
> ./maintenance.sh help create-missing-system-collections
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

As shown by its detailed help, the task
`create-missing-system-collections` needs a connection to a cluster
coordinator using a privileged user. To invoke it using a connection
to a specific endpoint and using a specific database user, use
`--server.endpoint` and `--server.username`, e.g.:

```
> ./maintenance.sh --server.endpoint tcp://domain:port --server.username admin create-missing-system-collections
```

If connecting to an SSL enabled server you need to use `ssl` instead
of `tcp` when specifying the server endpoint.

In order to make use of JWT tokens please add
`--server.ask-jwt-secret` to the command-line.

You can get your JWT in a kubernetes cluster with a command similar to
the following:

```
kubectl get secrets nameofyourdep-jwt -o json | jq -r '.data.token' | base64 -D
```

## Known Issues

Please heed warnings about undetermined server roles in older
versions, namely in the ArangoDB 3.3 series and ArangoDB before 3.4.6.
There we can not tell the server roles for sure so please check the
given endpoints twice.

## Creating an Agency Dump

In alternative to connecting to a leader agent, some of the scripts
will accept an agency dump JSON file as their input.

One can run the `dump` task against the cluster's leader agent, e.g.:

```
> ./maintenance.sh dump --server.endpoint AGENT
```

**Important:** do not use the coordinator route to create the agency
dump: create the dump connecting directly to the agency.
