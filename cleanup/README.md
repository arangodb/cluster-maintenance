# Usage

## Remove Zombies Script

    ./remove-zombies.sh --server.ask-jwt-secret --server.endpoint tcp://LEAD_AGENT:LEAD_AGENT_PORT ../analyze/zombies.json 

You need to connect to the **leading** agent. Use *ssl* in the *endpoint* instead of *tcp* if you are using SSL.

## Remove Dead-Primaries Script

    ./remove-dead-primaries.sh --server.ask-jwt-secret --server.endpoint tcp://LEAD_AGENT:LEAD_AGENT_PORT ../analyze/dead-primaries.json

You need to connect to the **leading** agent. Use *ssl* in the *endpoint* instead of *tcp* if you are using SSL.
