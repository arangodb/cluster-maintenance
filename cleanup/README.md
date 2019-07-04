# Usage

## Remove Zombies Script

    ./remove-zombies.sh --server.ask-jwt-secret --server.endpoint tcp://LEAD_AGENT:LEAD_AGENT_PORT ../analyze/zombies.json 

You need to connect to the **leading** agent. Use *ssl* in the *endpoint* instead of *tcp* if you are using SSL.

## Remove Dead-Primaries Script

    ./remove-dead-primaries.sh --server.ask-jwt-secret --server.endpoint tcp://LEAD_AGENT:LEAD_AGENT_PORT ../analyze/dead-primaries.json

You need to connect to the **leading** agent. Use *ssl* in the *endpoint* instead of *tcp* if you are using SSL.

## Create missing system collections script

This is a helper script to create missing system collections, in all databases.
It will iterate over the list of databases and check for the availability of the default system collections in them. 
It will create the missing system collections automatically.

The script is supposed to be used from inside the arangosh, with a privileged user (i.e. a user that has write privileges for all databases).

It is suitable for use with ArangoDB 3.4. It should **not** be used without adjustment for ArangoDB 3.5, as it will create too many system collections there
(we are not creating all of these system collections in 3.5 by default anymore).
