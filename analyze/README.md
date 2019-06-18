# Usage

    ./analyze.sh dump.json

This will print summaries for

* Primaries
* Databases
* Collections
* Primary Shards
* Zombie Collections

In case that there are left-over of failed collection creation (zombies), a file

    zombies.json

with details will be generated.

## creating a dump file

    curl -L -d '[["/"]]' -o dump.json http://AGENCY_SERVER:AGENCY_PORT/_api/agency/read 

This will use curl to get the dump file from an agent. You need to specify on
agency endpoint. In case this is not the leader *curl* will follow the redirects
which will point to the leader.
