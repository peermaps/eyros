const RAM = require('random-access-memory')
const eyros = require('../2d')
const fs = require('fs')
const test = require('tape')

test('2d', async function (t) {
  t.plan(1)
  var db = await eyros({
    storage: RAM,
    wasmSource: fs.readFileSync(require.resolve('../2d.wasm'))
  })
  await db.batch([
    { type:'insert', point:[+1,+2], value: Uint8Array.from([97,98,99]) },
    { type:'insert', point:[-5,+4], value: Uint8Array.from([100,101]) },
    { type:'insert', point:[+9,-3], value: Uint8Array.from([102,103,104]) },
    { type:'insert', point:[+5,-15], value: Uint8Array.from([105,106]) },
    { type:'insert', point:[[+5,+12],[-15,-3]], value: Uint8Array.from([107,108,109]) },
    { type:'insert', point:[[-20,-11],[-8,+2]], value: Uint8Array.from([110]) },
  ])
  var rows = await collect(await db.query([-10,-10,+10,+10]))
  t.deepEqual(rows.map(row => row.slice(0,2)).sort(cmp), [
    [ [+1,+2], Uint8Array.from([97,98,99]) ],
    [ [-5,+4], Uint8Array.from([100,101]) ],
    [ [+9,-3], Uint8Array.from([102,103,104]) ],
    [ [[+5,+12],[-15,-3]], Uint8Array.from([107,108,109]) ]
  ].sort(cmp))
})

function cmp (a, b) { return Buffer.compare(a[1], b[1]) }

async function collect (iter) {
  var row, rows = []
  while (row = await iter.next()) {
    rows.push(row)
  }
  return rows
}
