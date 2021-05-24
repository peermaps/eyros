const RAM = require('random-access-memory')
const eyros = require('../2d')
const fs = require('fs')
const test = require('tape')

test('errorIfMissing=default (true)', function (t) {
  t.plan(1)
  eyros({
    storage: RAM,
    wasmSource: fs.readFileSync(require.resolve('../2d.wasm'))
  }).then(ready).catch(err => { console.log('err=',err); t.error(err) })
  function ready(db) {
    db.batch([
      { type:'delete', point:[+1,+2], id: Uint8Array.from([1,2,3,4]) }
    ])
      .then(() => t.fail('should have failed'))
      .catch(err => t.ok('caught expected error'))
  }
})
test('errorIfMissing=true', function (t) {
  t.plan(1)
  eyros({
    storage: RAM,
    wasmSource: fs.readFileSync(require.resolve('../2d.wasm'))
  }).then(ready).catch(err => { console.log('err=',err); t.error(err) })
  function ready(db) {
    db.batch([
      { type:'delete', point:[+1,+2], id: Uint8Array.from([1,2,3,4]) }
    ])
      .then(() => t.fail('should have failed'))
      .catch(err => t.ok('caught expected error'))
  }
})

test('errorIfMissing=false', async function (t) {
  var db = await eyros({
    storage: RAM,
    wasmSource: fs.readFileSync(require.resolve('../2d.wasm'))
  })
  await db.batch([
    { type:'delete', point:[+1,+2], id: Uint8Array.from([1,2,3,4]) }
  ], { errorIfMissing: false })
  t.end()
})
