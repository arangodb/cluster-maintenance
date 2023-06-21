# ArangoDB Cluster Maintenance Tools

**The Cluster Maintenance tools can only be used against the Enterprise
Edition of ArangoDB. They must be used with caution, under the
guidance of ArangoDB support. It is possible to accidentally delete
all data. Do not use on your own.**

## Purpose

A collection of several tools that gather diagnostic information
about an ArangoDB cluster and also optionally apply modifications
to it.

## Prerequisites

In order to use these scripts, a working installation of the
ArangoShell (arangosh) is needed. The scripts currently support
ArangoDB versions 3.8, 3.9 and 3.10. Support for further upcoming stable 
ArangoDB releases will be added once they are released.

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
ARANGOSH=/path/to/arangosh ./maintenance.sh help
```

If the following error occurs, then it is also necessary to set the
path for the arangosh JavaScript code:

```
FATAL [3537a] {general} failed to locate javascript.startup-directory directory, its neither available in '/usr/share/arangodb3/js' nor in ...
```

The JavaScript path for the arangosh can be set via the 
`--javascript.startup-directory` option:

```
ARANGOSH=/path/to/arangosh ./maintenance.sh --javascript.startup-directory /path/to/js ...
```

For example:

```
ARANGOSH=/usr/local/bin/arangosh ./maintenance.sh --javascript.startup-directory /usr/local/share/arangodb3/js help
```

Please note that you might be asked for a password. For help, just press
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
  post-agency-plan                     3.3.23 - 3.9.99    Posts an agency dump to an ArangoDB agency leader.

analyze tasks:
  analyze                              3.3.23 - 3.9.99    Performs health analysis on your cluster and p  ...

move shard tasks:
  create-move-analysis                 3.3.23 - 3.9.99    Creates analysis for a plan to rebalance shards in your cluster.
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

Note: If you want to use the maintenance script via a Docker container, follow the next steps to create one:
Run `make docker` first to prepare everything and implement the makefile section for the Docker container. 
Then, navigate to the `containers` directory and run `docker build .`
This builds the most recent Docker container which will then for instance be usable on a Kubernetes cluster.

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

In order to make use of JWT tokens, please add
`--server.ask-jwt-secret` to the command-line.

You can get your JWT in a kubernetes cluster with a command similar to
the following:

```
kubectl get secrets nameofyourdep-jwt -o json | jq -r '.data.token' | base64 -D
```

## Running the `analyze` task

One of the most important tasks to run is the `analyze` task, which will produce
an overview of the cluster's current state.

The task can be run in two modes:
* using an already created agency dump, stored in a JSON file on disk
* using the current state of the cluster as provided by an agent or coordinator
  instance.

To run the task using an existing agency dump, please provide the filename to the
dump:
```
> ./maintenance.sh --server.endpoint COORDINATOR analyze ./the-agency-dump.json
```

When not providing a filename with an agency dump, the `analyze` task will use the
current cluster's state as provided by the agency or coordinator that is provided
on the command-line.

If the `analyze` task reports issues, it will write an output file to disk that can
be used with some repair/adjustment tasks later.
In this case the `analyze` task will print out follow-up instructions about which
other tasks to invoke next.

## Creating an Agency History

One can run the `history` task against the cluster's coordinator, e.g.:

```
> ./maintenance.sh --server.endpoint COORDINATOR history ./agency-history.json
```

## Creating an Agency Dump

As an alternative to connecting to a leader agent, some of the scripts
will accept an agency dump JSON file as their input.

One can run the `dump` task against the cluster's leader agent, e.g.:

```
> ./maintenance.sh --server.endpoint AGENT dump ./agency-dump.json
```

## Overriding version checks

Normally, all tasks provided by the cluster maintenance tools are restricted
to a range of compatible ArangoDB versions. If the version of the ArangoDB
instance the scripts are run against does not fall into the expected version
range, then some or even all tasks may not be available. In this case, there 
will be info messages about incompatible tasks at the start of the tool.

Here is a (made up) example for a version mismatch message:
```
analyze: removing this task because of mismatching shell version (3.6.16) requires: 3.3.23 - 3.5.99
```
Tasks outside the accepted version range will not be available unless the
version check is explicitly disabled. This can be necessary when running
an old version of the maintenance scripts against a newer version of ArangoDB,
or when using a not-yet-released preview version of ArangoDB.
In these cases the version check can be disabled via adding the option 
`--ignore-version`.

```
> ./maintenance.sh --server.endpoint ... help --ignore-version
```
## How to create a release

The following steps are needed:

 - Edit `VERSION` and bump the version number, commit and push.
 - Create a tag with `git tag -l v2.9.3 ; git push --tags`.
 - Run `make dist` to create 3 files in the `work` folder.
 - Under `Releases` in the github page of the repository, click 
   "Draft new release", create a description, choose your tag and
   upload/attach the three files newly generated by `make dist` in the
   `work` folder.
 - Click "Publish"

