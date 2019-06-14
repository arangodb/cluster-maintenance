const fs = require('fs');
const _ = require('underscore');

let shardsToMove = JSON.parse(fs.read("moveShards.txt"));

let amount = -1;
if (ARGUMENTS.length > 0) {
  try {
    amount = Number.parseInt(ARGUMENTS[0]);
  } catch (ignore) {
  }
}

let failed = 0;
let success = 0;
_.each(shardsToMove, function (shard) {
  if (amount > 0 || amount === -1) {
    try {
      print("Moving shard: " + shard.shard + " from: " + shard.fromServer + " to: " + shard.toServer);
      let res = arango.POST('/_admin/cluster/moveShard', shard);
      
      if (res.error) {
        print("Failed: " + res.errorNum.errorMessage);
        failed++;
      } else {
        success++;
      }
    } catch (e) {
      print(e);
      failed++;
    }
    if (amount > 0) {
      amount--;
    }
  }
});

print("Started " + success + " move jobs. (" + failed +" of them failed)")