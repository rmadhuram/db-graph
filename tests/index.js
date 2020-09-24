require('../src/config').readConfig()

var assert = require('assert');
var db = require('../src/db');
const { logger } = require('../src/logger');

async function wait(n) {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve()
    }, n)
  })
}

async function cleanupDB() {
  logger.info('Cleaning up database...')
  await db.executeQuery('delete from student')
  await db.executeQuery('delete from score')
  await db.executeQuery('delete from subject')
}

async function insertStudents(n) {
  let values = []
  for (let i = 0; i < n; i++) {
    values.push([[i+1], [i+1000], `student${i+1}`])
  }
  logger.info(`Inserting ${n} students`)
  await db.executeQuery(`insert into student (id, external_id, name) values ?`, [values])
}

async function insertSubjects(n) {
  let values = []
  for (let i = 0; i < n; i++) {
    values.push([[i+1], [i+1000], `subject${i+1}`])
  }
  logger.info(`Inserting ${n} subjects`)
  await db.executeQuery(`insert into subject (id, external_id, name) values ?`, [values])
}

async function insertScore(scores) {
  logger.info(`Inserting ${scores.length} scores`)
  await db.executeQuery(`insert into score (student_id, subject_id, score) values ?`, [scores])
}

function dummy() {
  // do nothing
}

describe('a simple graph with foreign keys', () => {
  let DB = require('../src/db-graph')
  let graph = new DB({
    'student': {
      pk: ['id'],
      columns: ['name'], 
      lastUpdatedColumn: 'last_updated',
      idMaps: ['external_id']
    },
    'subject': {
      pk: ['id'],
      columns: ['name'], 
      lastUpdatedColumn: 'last_updated',
      idMaps: ['external_id']
    },
    'score': {
      pk: ['student_id', 'subject_id'],
      columns: ['score'],
      lastUpdatedColumn: 'last_updated',
      fk: {
        'student_id': 'student.id',
        'subject_id': 'subject.id'
      }      
    }           
  }, {
    refreshInterval: 15000,
    updateWindowInSecs: 10
  })

  before(cleanupDB)

  after(function() {
    graph.destroy()
  })

  it('init graph', async function() {
    this.timeout(10000)
    await insertStudents(3)
    await insertSubjects(2)
    await insertScore([
      [1, 1, 92],
      [1, 2, 79]
    ])
    await graph.init()
    graph.showStats()
  })
})

describe('check with 100 student records', function() {

  let DB = require('../src/db-graph')
  let graph = new DB({
    'student': {
      pk: ['id'],
      columns: ['name'], 
      lastUpdatedColumn: 'last_updated',
      idMaps: ['external_id']
    }    
  }, {
    refreshInterval: 15000,
    updateWindowInSecs: 10
  })

  before(async function() {
    this.timeout(10000)
    await cleanupDB()
    await insertStudents(100)
  })

  after(function() {
    graph.destroy()
  })

  it('check if 100 records are retrieved', async function() {
    await graph.init()
    assert(graph['student'])
    assert.strictEqual(Object.keys(graph['student']).length, 100)
    assert.strictEqual(graph['student'][10]['name'], 'student10')

    let idMap = graph.getIdMap('student', 'external_id')
    assert.strictEqual(idMap[1000]['name'], 'student1')
  })

  it('add one more record', async () => {
    logger.info("Test: Waiting for 10s")
    await wait(10000)
    logger.info("Test: Insert 1 record")
    await db.executeQuery(`insert into student (id, external_id, name) values (101, 9999, 'test')`)
    logger.info("Test: Waiting for 8s")
    await wait(8000)
    assert.strictEqual(Object.keys(graph['student']).length, 101)
    assert.strictEqual(graph['student'][100]['name'], 'student100')
    assert.strictEqual(graph['student'][101]['name'], 'test')

    let idMap = graph.getIdMap('student', 'external_id')
    assert.strictEqual(idMap[9999]['name'], 'test')
  })
})

describe('large number of records (100,000)', function() {

  let DB = require('../src/db-graph')
  let graph = new DB({
    'student': {
      pk: ['id'],
      columns: ['name'], 
      lastUpdatedColumn: 'last_updated'
    }    
  }, {
    refreshInterval: 15000,
    updateWindowInSecs: 10
  })

  before(async function() {
    this.timeout(100000)
    await cleanupDB()
    await insertStudents(100000)
  })

  after(function() {
    graph.destroy()
  })

  it('check if 100000 records are retrieved', async function() {
    await graph.init()

    assert(graph['student'])
    assert.strictEqual(Object.keys(graph['student']).length, 100000)
    assert.strictEqual(graph['student'][10]['name'], 'student10')
  })
})