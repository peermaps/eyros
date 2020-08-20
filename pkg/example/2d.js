var RAM = require('random-access-memory')
var eyros = require('../2d')

;(async function () {
  var db = await eyros({
    storage: RAM,
    wasmSource: await (await fetch('2d.wasm')).arrayBuffer()
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
