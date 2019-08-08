# Usage

    ./analyze.sh dump.json

This will print summaries for

* Primaries
* Databases
* Collections
* Primary Shards
* Zombie Collections
* Dead Primaries in Current

In case that there are left-over of failed collection creation (zombies), a file

    zombies.json

with details will be generated.

In case that there are left-over of dead primaries in current, a file

    dead-primaries.json
    
with details will ge generated.

In case that there databases with missing system collections, a file

    missing-collections.json
    
with details will ge generated.

See the [cleanup](../cleanup/README.md) scripts for next steps

## Creating an Agency dump file

    curl -L -d '[["/"]]' -o dump.json http://AGENCY_SERVER:AGENCY_PORT/_api/agency/read 

This will use *curl* to get the dump file from an agent. You need to specify on
agency endpoint. In case this is not the leader *curl* will follow the redirects
which will point to the leader.

You might need the JWT token: see all details on how to generate an Agency dump at 
https://www.arangodb.com/docs/3.4/troubleshooting-cluster-agency-dump.html

**Important:** do not use the Coordinator route to create the agency dump: create the dump
connecting directly to the Agency
