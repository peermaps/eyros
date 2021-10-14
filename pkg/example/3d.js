const RAM = require('random-access-memory')
const eyros = require('../3d')

;(async function () {
  var db = await eyros({
    storage: RAM,
    wasmSource: await fetch('3d.wasm')
  })
  await db.batch([
    { type:'insert', point:[+1,+2,-3], value: Uint8Array.from([97,98,99]) },
    { type:'insert', point:[-5,+4,+5], value: Uint8Array.from([100,101]) },
    { type:'insert', point:[[+9,+14],-3,[+7,+10]], value: Uint8Array.from([102,103,104]) },
    { type:'insert', point:[+5,-15,-8], value: Uint8Array.from([105,106]) },
    { type:'insert', point:[[+4,+13],-7,[+11,+15]], value: Uint8Array.from([107]) },
  ])
  var q = await db.query([-10,-10,-10,+10,+10,+10])
  var row
  while (row = await q.next()) {
    console.log('row=',row)
  }
})()
