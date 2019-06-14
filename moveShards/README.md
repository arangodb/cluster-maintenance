# Create the transfer plan (endpoint: agency)
`arangosh --server.endpoint "tcp://<angency-address>:<agency-port>" --javascript.execute /Volumes/External/Git/IBM/createMovePlan.js`

The file `moveShards.txt` will be created. Inside you'll find the description of each move shard job in an array:
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

Currently there is a limit to 50k move shard jobs set (due to JavaScript String limitations).

# Execute the created move plan (endpoint: coordinator)
`arangosh --server.endpoint "tcp://<coordinator-address>:<coordinator-port>" --javascript.execute /Volumes/External/Git/IBM/executeMovePlan.js`

The result output will provide a bit more detail of the whole process.

```
...
Moving shard: s4015369 from: PRMR-0f311e4a-bcce-41d8-bff0-670fde2366be to: PRMR-49aad884-698e-452b-a19b-dead8c8f4e11
Moving shard: s4016713 from: PRMR-0f311e4a-bcce-41d8-bff0-670fde2366be to: PRMR-49aad884-698e-452b-a19b-dead8c8f4e11
Started 5 move jobs. (1 of them failed)
```
