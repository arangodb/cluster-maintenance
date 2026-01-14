/**
 * Unit tests for lib/analyze-functions.js
 * 
 * Test pattern:
 *   1. Load a base agency dump from fixtures
 *   2. Optionally modify it for the specific test case
 *   3. Feed it to the analysis function
 *   4. Assert on the results
 */

const { describe, it, assert, loadFixture, cloneDeep } = require('./test-runner');
const analyzeFunctions = require('../lib/analyze-functions');

// Helper to create the info object that many functions expect
function createBaseInfo(dump) {
  const info = {
    primaries: {}
  };
  
  // Extract primaries from dump (needed by many analysis functions)
  const health = dump.arango.Supervision.Health;
  for (const [key, server] of Object.entries(health)) {
    if (key.startsWith('PRMR') && server.Status === 'GOOD') {
      info.primaries[key] = server;
    }
  }
  
  return info;
}

// ============================================================================
// extractFailed tests
// ============================================================================

describe('extractFailed', () => {
  it('should find no failed instances in healthy cluster', () => {
    const dump = loadFixture('minimal-agency-dump.json');
    const info = {};
    
    analyzeFunctions.extractFailed(info, dump);
    
    assert.lengthOf(info.failedInstances, 0);
  });

  it('should detect failed server with tcp endpoint', () => {
    const dump = cloneDeep(loadFixture('minimal-agency-dump.json'));
    dump.arango.Supervision.Health['PRMR-003'] = {
      Status: 'FAILED',
      Endpoint: 'tcp://localhost:8532',
      ShortName: 'DBServer3'
    };
    const info = {};
    
    analyzeFunctions.extractFailed(info, dump);
    
    assert.lengthOf(info.failedInstances, 1);
    assert.equal(info.failedInstances[0], 'http://localhost:8532');
  });

  it('should detect failed server with ssl endpoint', () => {
    const dump = cloneDeep(loadFixture('minimal-agency-dump.json'));
    dump.arango.Supervision.Health['PRMR-003'] = {
      Status: 'FAILED',
      Endpoint: 'ssl://localhost:8532',
      ShortName: 'DBServer3'
    };
    const info = {};
    
    analyzeFunctions.extractFailed(info, dump);
    
    assert.lengthOf(info.failedInstances, 1);
    assert.equal(info.failedInstances[0], 'https://localhost:8532');
  });
});

// ============================================================================
// zombieCoordinators tests
// ============================================================================

describe('zombieCoordinators', () => {
  it('should find no zombies when Current matches Plan', () => {
    const dump = loadFixture('minimal-agency-dump.json');
    const info = {};
    
    const hasZombies = analyzeFunctions.zombieCoordinators(info, dump);
    
    assert.equal(hasZombies, false);
    assert.lengthOf(info.zombieCoordinators, 0);
  });

  it('should detect zombie coordinator in Current but not in Plan', () => {
    const dump = cloneDeep(loadFixture('minimal-agency-dump.json'));
    // Add coordinator to Current that is not in Plan
    dump.arango.Current.Coordinators['CRDN-ZOMBIE'] = {};
    const info = {};
    
    const hasZombies = analyzeFunctions.zombieCoordinators(info, dump);
    
    assert.equal(hasZombies, true);
    assert.lengthOf(info.zombieCoordinators, 1);
    assert.equal(info.zombieCoordinators[0], 'CRDN-ZOMBIE');
  });
});

// ============================================================================
// extractCollectionIntegrity tests
// ============================================================================

describe('extractCollectionIntegrity', () => {
  it('should report no issues for healthy cluster', () => {
    const dump = loadFixture('minimal-agency-dump.json');
    const info = createBaseInfo(dump);
    
    analyzeFunctions.extractCollectionIntegrity(info, dump);
    
    assert.lengthOf(info.noPlanDatabases, 0);
    assert.lengthOf(info.noShardCollections, 0);
    assert.lengthOf(info.realLeaderMissing, 0);
    assert.lengthOf(info.leaderOnDeadServer, 0);
    assert.lengthOf(info.followerOnDeadServer, 0);
  });

  it('should detect leader on dead server', () => {
    const dump = cloneDeep(loadFixture('minimal-agency-dump.json'));
    // Mark PRMR-001 as failed (remove from GOOD servers)
    dump.arango.Supervision.Health['PRMR-001'].Status = 'FAILED';
    
    const info = createBaseInfo(dump);
    analyzeFunctions.extractCollectionIntegrity(info, dump);
    
    // Some collections have PRMR-001 as leader
    assert.ok(info.leaderOnDeadServer.length > 0, 
      'Should detect leaders on dead server');
  });

  it('should detect follower on dead server', () => {
    const dump = cloneDeep(loadFixture('minimal-agency-dump.json'));
    // Mark PRMR-002 as failed (it's a follower on some shards)
    dump.arango.Supervision.Health['PRMR-002'].Status = 'FAILED';
    
    const info = createBaseInfo(dump);
    analyzeFunctions.extractCollectionIntegrity(info, dump);
    
    // Some collections have PRMR-002 as follower
    assert.ok(info.followerOnDeadServer.length > 0,
      'Should detect followers on dead server');
  });

  it('should detect database in Collections but not in Databases', () => {
    const dump = cloneDeep(loadFixture('minimal-agency-dump.json'));
    // Add a collection for a database that doesn't exist in Plan.Databases
    dump.arango.Plan.Collections['deletedDB'] = {
      '99999': {
        id: '99999',
        name: 'orphan',
        shards: { 's99999': ['PRMR-001'] }
      }
    };
    
    const info = createBaseInfo(dump);
    analyzeFunctions.extractCollectionIntegrity(info, dump);
    
    assert.ok(info.noPlanDatabases.length > 0,
      'Should detect orphaned database collections');
  });

  it('should detect missing distributeShardsLike leader', () => {
    const dump = cloneDeep(loadFixture('minimal-agency-dump.json'));
    // Add collection referencing non-existent prototype
    dump.arango.Plan.Collections['testdb']['30001'] = {
      id: '30001',
      name: 'dependent',
      distributeShardsLike: '99999', // doesn't exist
      shards: { 's30001': ['PRMR-001'] }
    };
    
    const info = createBaseInfo(dump);
    analyzeFunctions.extractCollectionIntegrity(info, dump);
    
    assert.lengthOf(info.realLeaderMissing, 1);
    assert.equal(info.realLeaderMissing[0].distributeShardsLike, '99999');
  });
});

// ============================================================================
// extractSupervisionLocks tests
// ============================================================================

describe('extractSupervisionLocks', () => {
  it('should find no locks in clean cluster', () => {
    const dump = loadFixture('minimal-agency-dump.json');
    const info = {};
    
    analyzeFunctions.extractSupervisionLocks(info, dump);
    
    assert.lengthOf(info.supervisionLocks, 0);
  });

  it('should detect DBServer write lock without pending job', () => {
    const dump = cloneDeep(loadFixture('minimal-agency-dump.json'));
    dump.arango.Supervision.DBServers = {
      'PRMR-001': 'orphan-job-123'
    };
    const info = {};
    
    analyzeFunctions.extractSupervisionLocks(info, dump);
    
    assert.lengthOf(info.supervisionLocks, 1);
    assert.equal(info.supervisionLocks[0].type, 'DBServer write lock');
    assert.equal(info.supervisionLocks[0].job, 'orphan-job-123');
  });

  it('should not report lock that has pending job', () => {
    const dump = cloneDeep(loadFixture('minimal-agency-dump.json'));
    dump.arango.Supervision.DBServers = {
      'PRMR-001': 'valid-job-123'
    };
    dump.arango.Target.Pending['valid-job-123'] = {};
    const info = {};
    
    analyzeFunctions.extractSupervisionLocks(info, dump);
    
    assert.lengthOf(info.supervisionLocks, 0);
  });
});

// ============================================================================
// extractSatelliteIssues tests
// ============================================================================

describe('extractSatelliteIssues', () => {
  it('should find no issues for normal collections', () => {
    const dump = loadFixture('minimal-agency-dump.json');
    const info = {};
    
    analyzeFunctions.extractSatelliteIssues(info, dump);
    
    assert.lengthOf(info.satelliteIssues, 0);
  });

  it('should detect satellite with replicationFactor 0 instead of "satellite"', () => {
    const dump = cloneDeep(loadFixture('minimal-agency-dump.json'));
    dump.arango.Plan.Collections['testdb']['40001'] = {
      id: '40001',
      name: 'badsatellite',
      replicationFactor: 0, // Should be "satellite"
      isSmart: false,
      shards: { 's40001': ['PRMR-001'] }
    };
    const info = {};
    
    analyzeFunctions.extractSatelliteIssues(info, dump);
    
    assert.lengthOf(info.satelliteIssues, 1);
    assert.equal(info.satelliteIssues[0].collection, 'badsatellite');
  });
});

// ============================================================================
// extractOutOfSyncFollowers tests
// ============================================================================

describe('extractOutOfSyncFollowers', () => {
  it('should find no out-of-sync followers in healthy cluster', () => {
    const dump = loadFixture('minimal-agency-dump.json');
    const info = {};
    
    analyzeFunctions.extractOutOfSyncFollowers(info, dump);
    
    assert.lengthOf(info.outOfSyncFollowers, 0);
  });

  it('should detect missing follower in Current', () => {
    const dump = cloneDeep(loadFixture('minimal-agency-dump.json'));
    // Plan says 2 servers, Current only has leader
    dump.arango.Current.Collections['testdb']['20001']['s20001'].servers = ['PRMR-001'];
    const info = {};
    
    analyzeFunctions.extractOutOfSyncFollowers(info, dump);
    
    assert.ok(info.outOfSyncFollowers.length > 0, 
      'Should detect out of sync followers');
  });
});

// ============================================================================
// recursiveMapPrinter tests
// ============================================================================

describe('recursiveMapPrinter', () => {
  it('should convert Map to plain object', () => {
    const map = new Map([['a', 1], ['b', 2]]);
    
    const result = analyzeFunctions.recursiveMapPrinter(map);
    
    assert.deepEqual(result, { a: 1, b: 2 });
  });

  it('should convert Set to array', () => {
    const set = new Set([1, 2, 3]);
    
    const result = analyzeFunctions.recursiveMapPrinter(set);
    
    assert.deepEqual(result, [1, 2, 3]);
  });

  it('should handle nested structures', () => {
    const nested = new Map([
      ['outer', new Map([['inner', 'value']])]
    ]);
    
    const result = analyzeFunctions.recursiveMapPrinter(nested);
    
    assert.deepEqual(result, { outer: { inner: 'value' } });
  });
});

