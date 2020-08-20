// replace "../" with "eyros/" in your project
const RAM = require('random-access-memory')
const eyros = require('../2d')
const fs = require('fs')

;(async function () {
  var db = await eyros({
    storage: RAM,
    wasmSource: fs.readFileSync(require.resolve('../2d.wasm'))
  })
  await db.batch([
    { type:'insert', point:[+1,+2], value: Uint8Array.from([97,98,99]) },
    { type:'insert', point:[-5,+4], value: Uint8Array.from([100,101]) },
    { type:'insert', point:[+9,-3], value: Uint8Array.from([102,103,104]) },
    { type:'insert', point:[+5,-15], value: Uint8Array.from([105,106]) },
    { type:'insert', point:[[+5,+12],[-15,-3]], value: Uint8Array.from([105,106]) },
    { type:'insert', point:[[-20,-11],[-8,+2]], value: Uint8Array.from([107]) },
  ])
  var q = await db.query([-10,-10,+10,+10])
  var row
  while (row = await q.next()) {
    console.log('row=',row)
  }
})()
