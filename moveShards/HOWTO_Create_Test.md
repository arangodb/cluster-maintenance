# How to create a test case

- create a cluster of 3 DB servers
- create a number of database
- in each database create a number of collections with RF 2
- shutdown on of the cluster and wait until it reports as bad and all shards have been moved
- bring the DB server up again, it should have no shards
- start the rebalance procedure
- check that the revived database server has shards
