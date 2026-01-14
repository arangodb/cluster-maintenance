/**
 * Minimal test runner for cluster-maintenance tests.
 * 
 * Usage:
 *   node test/test-runner.js [test-file-pattern]
 * 
 * Examples:
 *   node test/test-runner.js                          # Run all tests
 *   node test/test-runner.js test-analyze-functions   # Run specific test file
 */

const fs = require('fs');
const path = require('path');

// Colors for terminal output
const colors = {
  reset: '\x1b[0m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m'
};

class TestRunner {
  constructor() {
    this.tests = [];
    this.passed = 0;
    this.failed = 0;
    this.currentSuite = null;
  }

  /**
   * Define a test suite (group of related tests)
   */
  describe(name, fn) {
    this.currentSuite = name;
    console.log(`\n${colors.cyan}${name}${colors.reset}`);
    fn();
    this.currentSuite = null;
  }

  /**
   * Define a single test case
   */
  it(name, fn) {
    const fullName = this.currentSuite ? `${this.currentSuite} > ${name}` : name;
    try {
      fn();
      this.passed++;
      console.log(`  ${colors.green}✓${colors.reset} ${name}`);
    } catch (error) {
      this.failed++;
      console.log(`  ${colors.red}✗${colors.reset} ${name}`);
      console.log(`    ${colors.red}${error.message}${colors.reset}`);
      if (error.stack) {
        const stackLines = error.stack.split('\n').slice(1, 4);
        stackLines.forEach(line => {
          console.log(`    ${colors.yellow}${line.trim()}${colors.reset}`);
        });
      }
    }
  }

  /**
   * Print summary at the end
   */
  summary() {
    console.log('\n' + '─'.repeat(50));
    const total = this.passed + this.failed;
    console.log(`Tests: ${colors.green}${this.passed} passed${colors.reset}, ${colors.red}${this.failed} failed${colors.reset}, ${total} total`);
    return this.failed === 0;
  }
}

// Simple assertion functions
const assert = {
  equal(actual, expected, message) {
    if (actual !== expected) {
      throw new Error(message || `Expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
    }
  },

  deepEqual(actual, expected, message) {
    const actualStr = JSON.stringify(actual);
    const expectedStr = JSON.stringify(expected);
    if (actualStr !== expectedStr) {
      throw new Error(message || `Expected ${expectedStr}, got ${actualStr}`);
    }
  },

  strictEqual(actual, expected, message) {
    if (actual !== expected) {
      throw new Error(message || `Expected ${JSON.stringify(expected)} (strict), got ${JSON.stringify(actual)}`);
    }
  },

  ok(value, message) {
    if (!value) {
      throw new Error(message || `Expected truthy value, got ${JSON.stringify(value)}`);
    }
  },

  notOk(value, message) {
    if (value) {
      throw new Error(message || `Expected falsy value, got ${JSON.stringify(value)}`);
    }
  },

  throws(fn, message) {
    let threw = false;
    try {
      fn();
    } catch (e) {
      threw = true;
    }
    if (!threw) {
      throw new Error(message || 'Expected function to throw');
    }
  },

  arrayIncludes(array, item, message) {
    if (!array.includes(item)) {
      throw new Error(message || `Expected array to include ${JSON.stringify(item)}`);
    }
  },

  hasProperty(obj, prop, message) {
    if (!obj.hasOwnProperty(prop)) {
      throw new Error(message || `Expected object to have property '${prop}'`);
    }
  },

  lengthOf(array, length, message) {
    if (array.length !== length) {
      throw new Error(message || `Expected array length ${length}, got ${array.length}`);
    }
  }
};

/**
 * Load a JSON fixture file
 */
function loadFixture(name) {
  const fixturePath = path.join(__dirname, 'fixtures', name);
  const content = fs.readFileSync(fixturePath, 'utf8');
  return JSON.parse(content);
}

/**
 * Deep clone an object (for modifying fixtures without affecting original)
 */
function cloneDeep(obj) {
  return JSON.parse(JSON.stringify(obj));
}

// Create global instances for test files
const runner = new TestRunner();

// Export for use in test files
module.exports = {
  describe: runner.describe.bind(runner),
  it: runner.it.bind(runner),
  assert,
  loadFixture,
  cloneDeep,
  runner
};

// If run directly, execute all test files
if (require.main === module) {
  const testDir = __dirname;
  const pattern = process.argv[2] || '';
  
  const testFiles = fs.readdirSync(testDir)
    .filter(f => f.startsWith('test-') && f.endsWith('.js') && f !== 'test-runner.js')
    .filter(f => !pattern || f.includes(pattern));

  if (testFiles.length === 0) {
    console.log(`${colors.yellow}No test files found${pattern ? ` matching '${pattern}'` : ''}${colors.reset}`);
    process.exit(0);
  }

  console.log(`Running ${testFiles.length} test file(s)...\n`);

  testFiles.forEach(file => {
    console.log(`${colors.cyan}═══ ${file} ═══${colors.reset}`);
    require(path.join(testDir, file));
  });

  const success = runner.summary();
  process.exit(success ? 0 : 1);
}

