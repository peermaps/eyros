const RAM = require('random-access-memory')
const eyros = require('../2d')
const fs = require('fs')
const test = require('tape')
const randomBytes = require('crypto').randomBytes

test('insert+delete', async function (t) {
  t.plan(2)
  var db = await eyros({
    storage: RAM,
    wasmSource: fs.readFileSync(require.resolve(`../2d.wasm`))
  })
  var insertBatch = []
  for (var i = 0; i < 1000; i++) {
    var point = [
      (Math.random()*2-1)*15,
      (Math.random()*2-1)*15
    ]
    insertBatch.push({
      type: 'insert',
      point,
      value: Uint8Array.from(randomBytes(Math.floor(Math.random()*10+1)))
    })
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
    deleteBatch.push({
      type: 'delete',
      location: beforeRows[i][2]
    })
  }
  await db.batch(deleteBatch)

  var afterRows = await collect(await db.query(bbox))
  t.deepEqual(
    afterRows.map(row => [ round(row[0]), row[1] ]).sort(cmp),
    beforeRows
      .filter((row,i) => i%5 !== 0 && intersect(row[0], bbox))
      .map(row => [ round(row[0]), row[1] ])
      .sort(cmp)
  )
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
