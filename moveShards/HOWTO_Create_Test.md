# How to create a test case

- create a cluster of 3 dbservers
- create a number of database
- in each database create a number of collections with RF 2
- shutdown on of the dbserver and wait until it reports as bad and all shards have been moved
-- you might have to wait a while until all leaders and followers have appeared on the two remaining dbserver
- bring the DB server up again, it should have no shards
-- check that it has no (or only a few shards)
- start the rebalance procedure
- check that the revived database server has shards
