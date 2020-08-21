const RAM = require('random-access-memory')
const eyros = {
  2: require('../2d'),
  3: require('../3d'),
  4: require('../4d'),
  5: require('../5d'),
  6: require('../6d'),
  7: require('../7d'),
  8: require('../8d'),
}
const fs = require('fs')
const test = require('tape')
const randomBytes = require('crypto').randomBytes

test('dimension sweep', async function (t) {
  t.plan(7)
  for (var n = 2; n <= 8; n++) {
    var db = await eyros[n]({
      storage: RAM,
      wasmSource: fs.readFileSync(require.resolve(`../${n}d.wasm`))
    })
    var batch = []
    for (var i = 0; i < 1000; i++) {
      var point = []
      for (var j = 0; j < n; j++) {
        point.push((Math.random()*2-1)*15)
      }
      batch.push({
        type: 'insert',
        point,
        value: Uint8Array.from(randomBytes(Math.floor(Math.random()*10+1)))
      })
    }
    await db.batch(batch)

    var bbox = []
    for (var j = 0; j < n; j++) bbox.push(-10)
    for (var j = 0; j < n; j++) bbox.push(+10)

    var rows = await collect(await db.query(bbox))
    t.deepEqual(
      rows.map(row => [ round(row[0]), row[1] ]).sort(cmp),
      batch
        .filter(row => intersect(row.point, bbox))
        .map(row => [ round(row.point), row.value ])
        .sort(cmp)
    )
  }
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
