const RAM = require('random-access-memory')
const eyros = require('../2d')
const fs = require('fs')
const test = require('tape')
const randomBytes = require('crypto').randomBytes

function getId(value) { return value.slice(4,6) }

test('delete with custom ids', async function (t) {
  t.plan(3)
  var db = await eyros({
    storage: RAM,
    getId,
    wasmSource: fs.readFileSync(require.resolve(`../2d.wasm`))
  })
  var insertBatch = []
  for (var i = 0; i < 1000; i++) {
    var point = [
      (Math.random()*2-1)*15,
      (Math.random()*2-1)*15
    ]
    var value = Uint8Array.from(randomBytes(10))
    value[4] = i%256
    value[5] = Math.floor(i/256)
    insertBatch.push({ type: 'insert', point, value })
  }
  await db.batch(insertBatch)

  var bbox = [-10,-10,+10,+10]
  var beforeRows = await collect(await db.query(bbox))
  t.deepEqual(
    beforeRows.map(row => [ round(row[0]), row[1] ]).sort(cmp),
    insertBatch
      .filter(row => intersect(row.point, bbox))
      .map(row => [ round(row.point), row.value ])
      .sort(cmp)
  )

  var deleteBatch = []
  for (var i = 0; i < beforeRows.length; i+=5) {
    var value = beforeRows[i][1]
    deleteBatch.push({
      type: 'delete',
      point: beforeRows[i][0],
      id: getId(value)
    })
  }
  await db.batch(deleteBatch)

  var afterRows = await collect(await db.query(bbox))
  var expected = beforeRows
    .filter((row,i) => i%5 !== 0 && intersect(row[0], bbox))
    .map(row => [ round(row[0]), row[1] ])
    .sort(cmp)
  var observed = afterRows.map(row => [ round(row[0]), row[1] ]).sort(cmp)
  t.equal(observed.length, expected.length)
  t.deepEqual(observed, expected)
})

function intersect (x, bbox) {
  var dim = bbox.length/2
  for (var i = 0; i < dim; i++) {
    if (Array.isArray(x[i])) {
      if (x[i][0] > bbox[i+dim] || x[i][1] < bbox[i]) return false
    } else if (x[i] < bbox[i] || x[i] > bbox[i+dim]) {
      return false
    }
  }
  return true
}

function round (x) {
  if (Array.isArray(x)) return x.map(round)
  var n = 1e2
  return Math.round(x*n)/n
}

function cmp (a, b) {
  return JSON.stringify(a) < JSON.stringify(b) ? -1 : +1
}

async function collect (iter) {
  var row, rows = []
  while (row = await iter.next()) {
    rows.push(row)
  }
  return rows
}
