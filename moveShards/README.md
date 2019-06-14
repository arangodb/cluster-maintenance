*Note: This script is has the implicit assumption that there is one dbserver with few shards and two with many (e.g., 1000,1000,10000). If there are two dbserver with lower than average number of shards, you would need to run the script twice.*

# [Optional] Create unbalanced test distribution
Please check [here](HOWTO_Create_Test.md).
 
# Create the transfer plan (endpoint: agency)

In step one we create a move shards plan from the agency.

`arangosh --server.endpoint "tcp://<angency-address>:<agency-port>" --javascript.execute createMovePlan.js`

The file `moveShards.txt` will be created in the current directory. Inside you'll find the description of each move shard job in an array:
```
[
 ... 
  {
    "database": "testDatabase-31",
    "collection": "vertices9",
    "shard": "s4020427",
    "fromServer": "PRMR-0f311e4a-bcce-41d8-bff0-670fde2366be",
    "toServer": "PRMR-49aad884-698e-452b-a19b-dead8c8f4e11"
  }
 ...
]
```

*Note that* no move shard jobs are actually triggered in this stage. Further note that you need to use the agency endpoint of the *leader*.

Currently there is a limit of 50k move shard jobs set (due to JavaScript String limitations).

# Execute the created move plan (endpoint: coordinator)

In step two we use the coordinator to execute the plan from step one.

`arangosh --server.endpoint "tcp://<coordinator-address>:<coordinator-port>" --javascript.execute executeMovePlan.js`

The result output will provide a bit more detail of the whole process.

```
...
Moving shard: s4015369 from: PRMR-0f311e4a-bcce-41d8-bff0-670fde2366be to: PRMR-49aad884-698e-452b-a19b-dead8c8f4e11
Moving shard: s4016713 from: PRMR-0f311e4a-bcce-41d8-bff0-670fde2366be to: PRMR-49aad884-698e-452b-a19b-dead8c8f4e11
Started 5 move jobs. (1 of them failed)
```

Note that this will actually trigger the move shard jobs. Further note that you can use the corrdinator endpoint of any active coordintor.

## Limit number of changes

`arangosh --server.endpoint "tcp://<coordinator-address>:<coordinator-port>" --javascript.execute executeMovePlan.js N`

This will execute the first N move shard jobs from the file.
