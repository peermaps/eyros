var RAM = require('random-access-memory')
var eyros = require('./eyros.js')
var Buffer = require('buffer').Buffer

window.log = function (msg) {
  console.log(msg)
}

;(async function () {
  var db = await eyros.open(function (x) {
    var r = new RAM()
    var {read,write} = r
    r.read = function (offset, length, cb) {
      return read.call(r, offset,length,cb)
    }
    r.write = function (offset, buf, cb) {
      if (!Buffer.isBuffer(buf)) buf = Buffer.from(buf)
      return write.call(r, offset, buf, cb)
    }
    r.len = function (cb) { cb(null, r.length) }
    return r
  })
  await db.batch([
    { type:'insert', point:[+1,+2], value: Uint8Array.from([97,98,99]) },
    { type:'insert', point:[-5,4], value: Uint8Array.from([100,101]) },
    { type:'insert', point:[+9,-3], value: Uint8Array.from([102,103,104]) },
    { type:'insert', point:[+5,-15], value: Uint8Array.from([105,106]) },
  ])
  var q = await db.query([-10,-10,+10,+10])
  var row
  while (row = await q.next()) {
    console.log('row=',row)
  }
})()
